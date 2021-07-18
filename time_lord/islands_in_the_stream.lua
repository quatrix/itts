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
    local timestamp = ARGV[3]
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

    merge_to_segments_continues(timestamp)
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
