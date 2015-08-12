module("shard.utils", package.seeall)

DEBUG = os.getenv('SHARD_DEBUG') or 0
DEBUG = DEBUG + 0

DEBUG_INDENTIATION = 4

function debug(msg, indent)
    if (DEBUG > 0) then
        if (not indent) then
            indent = 0
        end
        print(string.rep(" ", DEBUG_INDENTIATION * indent) .. msg)
    end
end

--- Convert the given table to a string.
-- @param maxDepth until this depth all tables contained as values in the table will be resolved.
-- @param valueDelimiter (optional) default is " = "
-- @param lineDelimiter (optional) default is "\n"
-- @param indent (optional) initial indentiation of sub tables
function convertTableToString(tab, maxDepth, valueDelimiter, lineDelimiter, indent)
    local result = ""
    if (indent == nil) then
        indent = 0
    end
    if (valueDelimiter == nil) then
        valueDelimiter = " = "
    end
    if (lineDelimiter == nil) then
        lineDelimiter = "\n"
    end
    for k, v in pairs(tab) do
        if (result ~= "") then
            result = result .. lineDelimiter
        end
        result = result .. string.rep(" ", indent) .. tostring(k) .. valueDelimiter
        if (type(v) == "table") then
            if (maxDepth > 0) then
                result = result .. "{\n" .. convertTableToString(v, maxDepth - 1, valueDelimiter, lineDelimiter, indent + 2) .. "\n"
                result = result .. string.rep(" ", indent) .. "}"
            else
                result = result .. "[... more table data ...]"
            end
        elseif (type(v) == "function") then
            result = result .. "[function]"
        else
            result = result .. tostring(v)
        end
    end
    return result
end

--- Calculate a simple checksum by simply summing up all byte values of
-- the given value (treated as string).
-- @param maxValue (optional, 65535 is default) the maximum value to be returned
-- @return a value between 0 and maxValue
function calculateSimpleChecksum(value, maxValue)
    value = tostring(value)
    if (maxValue == nil) then
        maxValue = 65535
    end
    local result = 0
    for a = 1, string.len(value) do
        result = result + string.byte(value, a)
        if (result > maxValue) then
            result = result - maxValue
        end
    end
    return result
end

--- Copy a file.
--
-- The file will be loaded into memory first so DO NOT USE THIS METHOD FOR LARGE FILES!
--
-- @param src filename of the source
-- @param dest filename of the destination
function copyFile(src, dest)
	local srcFd = assert(io.open(src, "rb"))
	local content = srcFd:read("*a")
	srcFd:close();

	local destFd = assert(io.open(dest, "wb+"))
	destFd:write(content);
	destFd:close();
end
