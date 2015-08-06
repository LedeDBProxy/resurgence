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

---  Encapsulate all the configuration logic.</br>
--
-- Configuration files are searched in the following places:
-- <ol>
--     <li>global variable <code>hscaleConfigFile</code></li>
--     <li>environment variable <code>HSCALE_CONFIG_FILE</code></li>
--     <li><code>/etc/hscale.lua</code></li>
-- </ol>
-- 
-- Configuration files must be written in Lua and must return a table with all the configuration.
-- 
-- @author $Author: peter.romianowski $
-- @release $Date: 2008-04-11 23:30:14 +0200 (Fr, 11 Apr 2008) $ $Rev: 30 $

module("optivo.hscale.config", package.seeall)

HSCALE_VERSION = "0.1"

local utils = require("optivo.common.utils")
local ENV_CONFIG_FILE = "HSCALE_CONFIG_FILE"

local _values = nil
local _sharding_table = {}

--- Query the value of the given key.
-- @return the value of the given configuration key. An error is thrown if no value has been set.
function get(key)
    assert(_values, "No configuration loaded.")
    --assert(_values[key] ~= nil, "Invalid configuration option '" .. key .. "'")
    return _values[key]
end

--- Get all configuration values.
-- @return a table with all configuration values.
function getAll()
    return _values
end

function getShardingTable()
    return _sharding_table
end
--- Test the given key.
-- @return true if the given key exists.
function contains(key)
    assert(_values, "No configuration loaded.")
    return _values[key] ~= nil
end

-- Load the configuration.
function _load()
    local configFile = _G["hscaleConfigFile"]
    if (not configFile) then
        configFile = os.getenv(ENV_CONFIG_FILE)
    end
    if (not configFile) then
        configFile = "/etc/hscale.lua"
    end
     utils.debug("Using config file '" .. configFile .. "'")
    local success, result = pcall(loadfile, configFile)
    assert(success and result ~= nil, "Error loading configuration file '" .. configFile .. "': " .. (tostring(result) or ""))
     utils.debug("Configuration '" .. configFile .. "' loaded.")
    _values = result()

    print("sharding num" .. #config.sharding_list)

    for i = 1, #config.sharding_list do
        _sharding_table[config.sharding_list[i].table] = config.sharding_list[i]
    end

    assert(_values, "Invalid configuration format in file '" .. configFile .."'.")
end

-- Load the configuration upon first import.
_load()
