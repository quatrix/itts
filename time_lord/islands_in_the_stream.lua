-- redis-priority-queue
-- Author: Gabriel Bordeaux (gabfl)
-- Github: https://github.com/gabfl/redis-priority-queue
-- Version: 1.0.4
-- (can only be used in 3.2+)


-- returns true if empty or null
-- http://stackoverflow.com/a/19667498/50501
local function isempty(s)
    return s == nil or s == '' or type(s) == 'userdata'
end



-- Get mandatory vars
local action = ARGV[1];
local key = ARGV[2];

-- Making sure required fields are not nil
assert(not isempty(action), 'ERR1: Action is missing')
assert(not isempty(key), 'ERR2: Queue name is missing')

local slicesKey = key .. "::slices"
local segmentsKey = key .. "::segments"


local function serialize_slice(timestamp, status)
    return cmsgpack.pack({timestamp, status})
end

local function serialize_segment(start_time, end_time, status)
    -- TODO: this is just to get this off the ground
    -- no reason not to use some binary serialization
    -- for example: https://msgpack.org/index.html
    -- return cmsgpack.pack({start_time, end_time, status})
    -- return start_time .. ':' .. end_time .. ':' .. status
    return cjson.encode({['start']=start_time, ['end']=end_time, ['status']=status})
end

local function deserialize_segment(segment)
    return cjson.decode(segment)
end

local function deserialize_slice(slice)
    return cmsgpack.unpack(slice)
end

local function merge_to_segments()
    redis.call('DEL', segmentsKey)

    local slices = redis.call('ZRANGEBYSCORE', slicesKey, '-inf', '+inf')
    -- local slices = redis.call('ZRANGE', slicesKey, '0', '-1')
    -- local slices = redis.call('ZSCAN', slicesKey, '0', '-1')

    local last_status = nil
    local start_time = nil
    local end_time = nil

    if table.getn(slices) == 0 then
        return
    end

    for k, slice in ipairs(slices) do 
        -- redis.log(redis.LOG_WARNING, 'slice: ', slice)
        slice = deserialize_slice(slice)

        local timestamp = slice[1]
        local status = slice[2]

        -- redis.log(redis.LOG_WARNING, 'timestamp: ', timestamp, ' status: ', status)

        if last_status == nil then
            last_status = status
            start_time = timestamp
            end_time = timestamp
        elseif last_status == status then
            end_time = timestamp
        elseif last_status ~= status then
            -- XXX: we could make the segment extend to the last known slice
            -- of the current status (by passing end_time), 
            -- or extend to the start of the next status (by passing timestamp)

            local serialized_segment = serialize_segment(start_time, end_time, last_status)
            redis.call('ZADD', segmentsKey, start_time, serialized_segment)

            last_status = status
            start_time = timestamp
            end_time = timestamp
        end
    end

    local serialized_segment = serialize_segment(start_time, end_time, last_status)
    redis.call('ZADD', segmentsKey, start_time, serialized_segment)
end




local function extend(a, b)
    local result = {}

    for k,v in pairs(a) do
        table.insert(result, v)
    end

    for k,v in pairs(b) do
         table.insert(result, v)
    end

    return result
end

local function reverse(t)
    for i = 1, math.floor(#t/2) do
        local j = #t - i + 1
        t[i], t[j] = t[j], t[i]
    end

    return t
end


local function get_ids_around_slice(id)
    local before = redis.call('ZREVRANGEBYSCORE', segmentsKey, id, 0, 'LIMIT', 0, 2)
    local after = redis.call('ZRANGEBYSCORE', segmentsKey, id, 'inf', 'LIMIT', 0, 2)

    local first_id = before[#before] 
    local last_id = after[#after] 

    if first_id == nil then
        first_id = id
    else
        first_id = deserialize_segment(first_id)['start']
    end

    if last_id == nil then
        last_id = 'inf'
    else
        last_id = deserialize_segment(last_id)['end']
    end

    -- redis.log(redis.LOG_WARNING, 'first_id: ', first_id)
    -- redis.log(redis.LOG_WARNING, 'last_id: ', last_id)

    return first_id, last_id
end

local function merge_to_segments_continues(new_slice_id)
    -- IDEA:
    -- we can remove the segments near `new_slice_id`
    -- and then get the slices near `new_slice_id` and add their segments
    --
    -- what could go wrong?

    local first_id, last_id = get_ids_around_slice(new_slice_id)

    -- removing segments
    local segments = redis.call('ZRANGEBYSCORE', segmentsKey, first_id, last_id)

    redis.log(redis.LOG_WARNING, 'segments: ', table.getn(segments))

    for k, segment in ipairs(segments) do 
        redis.call('ZREM', segmentsKey, segment)
    end

    -- local before = redis.call('ZREVRANGEBYSCORE', slicesKey, new_slice_id, 0, 'LIMIT', 0, 2)
    -- local after = redis.call('ZRANGEBYSCORE', slicesKey, new_slice_id, 'inf', 'LIMIT', 0, 2)
    -- local slices = extend(reverse(before), after)
    local slices = redis.call('ZRANGEBYSCORE', slicesKey, first_id, last_id)

    redis.log(redis.LOG_WARNING, 'slices: ', table.getn(slices))

    -- local slices = redis.call('ZRANGE', slicesKey, '0', '-1')
    -- local slices = redis.call('ZSCAN', slicesKey, '0', '-1')

    local last_status = nil
    local start_time = nil
    local end_time = nil

    if table.getn(slices) == 0 then
        return
    end

    for k, slice in ipairs(slices) do 
        -- redis.log(redis.LOG_WARNING, 'slice: ', slice)
        slice = deserialize_slice(slice)

        local timestamp = slice[1]
        local status = slice[2]

        -- redis.log(redis.LOG_WARNING, 'timestamp: ', timestamp, ' status: ', status)

        if last_status == nil then
            last_status = status
            start_time = timestamp
            end_time = timestamp
        elseif last_status == status then
            end_time = timestamp
        elseif last_status ~= status then
            -- XXX: we could make the segment extend to the last known slice
            -- of the current status (by passing end_time), 
            -- or extend to the start of the next status (by passing timestamp)

            local serialized_segment = serialize_segment(start_time, end_time, last_status)
            redis.call('ZADD', segmentsKey, start_time, serialized_segment)

            last_status = status
            start_time = timestamp
            end_time = timestamp
        end
    end

    local serialized_segment = serialize_segment(start_time, end_time, last_status)
    redis.call('ZADD', segmentsKey, start_time, serialized_segment)
end

local function insert_slice()
    local id = ARGV[3]
    local timestamp = ARGV[4]
    local status = ARGV[5]
    
    assert(not isempty(id), 'ERR1: id is missing')
    assert(not isempty(timestamp), 'ERR1: timestamp is missing')
    assert(not isempty(status), 'ERR1: status is missing')

    -- checking if we already have something stored for this id
    -- we're assuming id is unique per key
    local slice = redis.call('ZRANGEBYSCORE', slicesKey, id, id, "LIMIT", 0, 1)

    if slice[1] then
        -- redis.log(redis.LOG_WARNING, 'deleting slice: ', slice[1])
        redis.call('ZREM', slicesKey, slice[1])
    end

    -- redis.log(redis.LOG_WARNING, 'zadd ', id, ' slice ', serialize_slice(timestamp, status))
    redis.call('ZADD', slicesKey, 'NX', id, serialize_slice(timestamp, status))

    redis.log(redis.LOG_WARNING, 'id ', id, ' timestamp ', timestamp)
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
