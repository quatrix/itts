-- returns true if empty or null
-- http://stackoverflow.com/a/19667498/50501
local function isempty(s)
    return s == nil or s == '' or type(s) == 'userdata'
end

local action = ARGV[1];
local key = ARGV[2];

assert(not isempty(action), 'ERR1: action is missing')
assert(not isempty(key), 'ERR2: key is missing')

local slicesKey = key .. "::slices"
local segmentsKey = key .. "::segments"


local function serialize_segment(start_time, end_time, status)
    -- XXX: maybe we can save a little by using msgpack
    return cjson.encode({['start']=start_time, ['end']=end_time, ['status']=status})
end

local function deserialize_segment(segment)
    return cjson.decode(segment)
end

local function serialize_slice(timestamp, status)
    return cmsgpack.pack({timestamp, status})
end

local function deserialize_slice(slice)
    return cmsgpack.unpack(slice)
end

local function get_segments_range(timestamp)
    local before = redis.call('ZREVRANGEBYSCORE', segmentsKey, timestamp, 0, 'LIMIT', 0, 2)
    local after = redis.call('ZRANGEBYSCORE', segmentsKey, timestamp, 'inf', 'LIMIT', 0, 2)

    local t_start = before[#before] 
    local t_end = after[#after] 

    if t_start == nil then
        t_start = timestamp
    else
        t_start = deserialize_segment(t_start)['start']
    end

    if t_end == nil then
        t_end = 'inf'
    else
        t_end = deserialize_segment(t_end)['end']
    end

    return t_start, t_end
end

local function remove_segments(t_start, t_end)
    local segments = redis.call('ZRANGEBYSCORE', segmentsKey, t_start, t_end)

    for k, segment in ipairs(segments) do 
        redis.call('ZREM', segmentsKey, segment)
    end
end

local function get_slices(t_start, t_end)
    return redis.call('ZRANGEBYSCORE', slicesKey, t_start, t_end)
end

local function reverse(t)
    for i = 1, math.floor(#t/2) do
        local j = #t - i + 1
        t[i], t[j] = t[j], t[i]
    end

    return t
end

local function extend(a, b)
    local result = {}

    for k,v in ipairs(a) do
        table.insert(result, v)
    end

    for k,v in ipairs(b) do
         table.insert(result, v)
    end

    return result
end

local function get_surrounding_segments(timestamp)
    local before = redis.call('ZREVRANGEBYSCORE', segmentsKey, timestamp, 0, 'LIMIT', 0, 2)
    local after = redis.call('ZRANGEBYSCORE', segmentsKey, timestamp, 'inf', 'LIMIT', 0, 2)

    return extend(reverse(before), after)
end

local function get_surrounding_slices(timestamp)
    local before = redis.call('ZREVRANGEBYSCORE', slicesKey, '('..timestamp, 0, 'LIMIT', 0, 1)
    local after = redis.call('ZRANGEBYSCORE', slicesKey, '('..timestamp, 'inf', 'LIMIT', 0, 1)
    before = before[#before]
    after = after[#after]



    return before, after
end

local function _add_segment(start_time, end_time, status)
    assert(end_time >= start_time, 'ERR: end_time can\'t be before start_time')

    local serialized_segment = serialize_segment(start_time, end_time, status)
    redis.call('ZADD', segmentsKey, start_time, serialized_segment)
end

local function add_slice_to_segments(timestamp, status)
    -- When a new slice arrives find the segments closes to it
    -- * if it's same status and after/before them, merge them with it
    -- * if it's a different status, add a new segment after/before it
    -- * if it's same status in the middle of a segment - do nothing
    -- * if it's a different status in the middle of a segment get the slices before and after 
    --   it, remove the segment and create 3 new segments [before][now][after] using the slices.

    redis.log(redis.LOG_WARNING, '[add_slice] timestamp: ', timestamp, ' status: ', status)
    
    -- get all segments around this timestamp
    local segments = get_surrounding_segments(timestamp)

    for k, segment in ipairs(segments) do 
        redis.log(redis.LOG_WARNING, '[segment] timestamp: ', segment)
    end

    -- if there are no segments, add the first segment
    if table.getn(segments) == 0 then
        _add_segment(timestamp, timestamp, status)
        return
    end

    -- if there are segments, find where where to put this slice

    local closest_segment = nil
    local prev_segment = nil

    for k, segment in ipairs(segments) do 
        local _segment = deserialize_segment(segment)
        local s_start = tonumber(_segment['start'])
        local s_end = tonumber(_segment['end'])
        local s_status = _segment['status']

        -- if this segments starts after current slice
        -- it means all other segments would do to
        -- so our best bet is to insert before it
        if s_start > timestamp then
            -- let's check the prev segment, maybe it's better
            if prev_segment ~= nil then
                local _prev_segment = deserialize_segment(prev_segment)
                local ps_start = tonumber(_prev_segment['start'])
                local ps_end = tonumber(_prev_segment['end'])
                local ps_status = _prev_segment['status']

                if ps_status == status then
                    redis.call('ZREM', segmentsKey, prev_segment)
                    _add_segment(ps_start, timestamp, status)
                    return
                end
            end

            redis.log(redis.LOG_WARNING, '[HERE] ', segment)
            if s_status == status then
                -- redis.log(redis.LOG_WARNING, '[remove] ', segment)
                redis.call('ZREM', segmentsKey, segment)
                _add_segment(timestamp, s_end, status)
            else
                _add_segment(timestamp, timestamp, status)
            end

            prev_segment = segment
            return
        end

        -- if out slice is within the current segment, great
        -- let's put it within
        if s_start <= timestamp and s_end >= timestamp then
            -- if we're in the middle of a segment and we're the same status
            -- we don't do anything .
            -- FIXME: unless we want to store number of slices in a segment

            if s_status ~= status then
                -- but if it's not the same status, we need to split it!
                -- basically we need to find the first slice before timestamp
                -- and the first slice after timestamp
                local slice_before, slice_after = get_surrounding_slices(timestamp)

                redis.call('ZREM', segmentsKey, segment)

                if slice_before == nil and slice_after == nil then
                    _add_segment(timestamp, timestamp, status)
                    prev_segment = segment
                    return
                end

                if slice_before == nil then
                    -- if no slice before, it means we're ON the slice
                    slice_after = deserialize_slice(slice_after)
                    
                    _add_segment(timestamp, timestamp, status)
                    _add_segment(slice_after[1], s_end, s_status)
                    prev_segment = segment
                    return
                end

                slice_before = deserialize_slice(slice_before)

                if slice_after == nil then 
                    _add_segment(s_start, slice_before[1], s_status)
                    _add_segment(timestamp, timestamp, status)
                    prev_segment = segment
                    return
                end

                slice_after = deserialize_slice(slice_after)

                _add_segment(s_start, slice_before[1], s_status)
                _add_segment(timestamp, timestamp, status)
                _add_segment(slice_after[1], s_end, s_status)
            end

            prev_segment = segment
            return
        end

        -- if we didn't find a good segment, it means we should probably
        -- consider the last segment as the closest one?
        closest_segment = segment
        prev_segment = segment
    end

    -- if we made it here, means current slice is after all segments
    local _segment = deserialize_segment(closest_segment)
    local s_start = tonumber(_segment['start'])
    local s_end = tonumber(_segment['end'])
    local s_status = _segment['status']

    if s_status == status then
        redis.call('ZREM', segmentsKey, closest_segment)
        _add_segment(s_start, timestamp, status)
    else
        _add_segment(timestamp, timestamp, status)
    end
end

local function merge_to_segments_continues(timestamp)
    local t_start, t_end = get_segments_range(timestamp)
    local slices = get_slices(t_start, t_end)

    if table.getn(slices) == 0 then
        return
    end

    remove_segments(t_start, t_end)

    local slice = deserialize_slice(table.remove(slices, 1))
    local timestamp = slice[1]
    local status = slice[2]

    local start_time = timestamp
    local end_time = timestamp
    local last_status = status

    local function add_segment()
        -- XXX: we could make the segment extend to the last known slice
        -- of the current status (by passing end_time), 
        -- or extend to the start of the next status (by passing timestamp of the current slice)

        local serialized_segment = serialize_segment(start_time, end_time, last_status)
        redis.call('ZADD', segmentsKey, start_time, serialized_segment)
    end

    for k, slice in ipairs(slices) do 
        slice = deserialize_slice(slice)
        timestamp = slice[1]
        status = slice[2]

        if last_status == status then
            end_time = timestamp
        else
            add_segment()

            last_status = status
            start_time = timestamp
            end_time = timestamp
        end
    end

    -- don't forget last segment
    add_segment()
end

local function insert_slice()
    local timestamp = tonumber(ARGV[3])
    local status = ARGV[4]
    
    assert(not isempty(timestamp), 'ERR1: timestamp is missing')
    assert(not isempty(status), 'ERR1: status is missing')

    -- checking if we already have something stored for this timestamp
    -- we're assuming timestamp is unique per key
    local slice = redis.call('ZRANGEBYSCORE', slicesKey, timestamp, timestamp, "LIMIT", 0, 1)

    if slice[1] then
        redis.call('ZREM', slicesKey, slice[1])
    end

    redis.call('ZADD', slicesKey, 'NX', timestamp, serialize_slice(timestamp, status))

    -- merge_to_segments_continues(timestamp)
    add_slice_to_segments(timestamp, status)
end

local function get_segments()
    local segment_start = ARGV[3]
    local segment_end = ARGV[4]
    
    assert(not isempty(segment_start), 'ERR1: segment_start is missing')
    assert(not isempty(segment_end), 'ERR1: segment_end is missing')

    return redis.call('ZRANGEBYSCORE', segmentsKey, segment_start, segment_end)
end


if action == 'insert_slice' then
    return insert_slice()
elseif action == 'get_segments' then
    return get_segments()
elseif action == 'merge_to_segments' then
    return merge_to_segments()
else
    error('ERR3: Invalid action.')
end
