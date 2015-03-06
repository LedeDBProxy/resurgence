--[[
   Copyright (C) 2008 optivo GmbH

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
--]]

--- Utility functions like logging etc.
--
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-05-06 21:30:04 +0200 (Di, 06 Mai 2008) $ $Rev: 45 $

module("optivo.common.utils", package.seeall)

DEBUG = os.getenv('HSCALE_DEBUG') or 0
DEBUG = DEBUG + 0

DEBUG_INDENTIATION = 4

--- Print out a debug message (if DEBUG > 0) with the given indentiation
-- @param indent (optional) the number of spaces added before the message.
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
