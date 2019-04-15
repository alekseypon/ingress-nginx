local util = require("util")

local _M = { name = "dynamic_round_robin" }

function _M.new(self, backend)
  local o = {
    peers = backend.endpoints
  }
  setmetatable(o, self)
  self.__index = self
  return o
end

function _M.balance(self)
  local peers = self.peers
  local endpoint = peers[1]

  return endpoint.address .. ":" .. endpoint.port
end

function _M.sync(self, backend)
  local changed = not util.deep_compare(self.peers, backend.endpoints)
  if not changed then
    return
  end

  self.peers = backend.endpoints
end

return _M
