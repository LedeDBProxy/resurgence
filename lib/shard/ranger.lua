module("shard.ranger", package.seeall)

Ranger = {}
Ranger.__index = Ranger

function Ranger.create()
    local self = {}
    setmetatable(self, Ranger)

    self._min = -1
    self._max = -1

    return self
end

function Ranger:getRangeMinValue()
    return self._min
end

function Ranger:getRangeMaxValue()
    return self._max
end

function Ranger:setRangeValue(opType, rangeValue)
    if opType == "TK_LT" then
        rangeValue = rangeValue - 1
        opType = "TK_LE"
    elseif opType == "TK_GT" then
        rangeValue = rangeValue + 1
        opType = "TK_GE"
    end

    if opType == "TK_LE" then
        self._max = rangeValue
    elseif opType == "TK_GE" then
        self._min = rangeValue
    else
        self._min = rangeValue
        self._max = rangeValue
    end
end

