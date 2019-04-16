local util = require("util")
local math1 = require("math")
local math_max = math.max
local math_gcd = math1.gcd


-- Copyright (C) by Jianhao Dai (Toruneko)

local bit = require "bit"

local lshift = bit.lshift
local rshift = bit.rshift
local band = bit.band

local function iseven(x)
    return band(x, 1) == 0
end

local function gcd_math(x, y)
    if x < y then
        return gcd_math(y, x)
    end

    if y == 0 then
        return x
    end

    if iseven(x) then
        if iseven(y) then
            -- gcd(x >> 1, y >> 1) << 1
            return lshift(gcd_math(rshift(x, 1), rshift(y, 1)), 1)
        else
            return gcd_math(rshift(x, 1), y)
        end
    else
        if iseven(y) then
            return gcd_math(x, rshift(y, 1))
        else
            return gcd_math(y, x - y)
        end
    end
end



local _M = { name = "dynamic_round_robin" }

local function getmax(peers)
    local max = 0
    for _, peer in ipairs(peers) do
        max = math_max(max, peer.weight)
    end
    return max
end

local function getgcd(peers)
    local gcd1 = 0
    for _, peer in ipairs(peers) do
        gcd1 = gcd_math(peer.weight, gcd1)
    end
    return gcd1
end

local function get_weighted_round_robin_peer(ups)
    if ups.size == 1 then
        return ups.peers[1]
    end

    local cp = ups.cp
    local cw = ups.cw

    while true do
        ups.cp = (ups.cp % ups.size) + 1
        if ups.cp == 1 then
            ups.cw = ups.cw - ups.gcd
            if ups.cw <= 0 then
                ups.cw = ups.max
            end
        end

        local peer = ups.peers[ups.cp]

        if not peer then
            return nil, "no peer found: " .. ups.name
        end

        if peer.weight >= ups.cw then
            return peer
        end
        -- visit all peers, but no one avaliable, exit.
        if ups.cw == cw and ups.cp == cp then
            return nil, "no available peer: " .. ups.name
        end
    end
end

local function build_peers(backend)
  local peers = {}
  local weight
  for _, peer in pairs(backend.endpoints) do
    local name = peer.address .. ":" .. peer.port
    if backend.agent_checks_weights and backend.agent_checks_weights[name] then
      weight = backend.agent_checks_weights[name]
    else
      weight = 100
    end
    if weight > 0 then
      peers[#peers + 1] = {
        name = name,
        host = peer.address,
        port = tonumber(peer.port),
        weight = weight,
      }
    end
  end
  return peers
end

function _M.new(self, backend)
  local peers = build_peers(backend)
  local max = getmax(peers)
  local gcd = getgcd(peers)
  local o = {
    cp = 1, -- current peer index
    size = #peers, -- peers count size
    gcd = gcd, -- GCD
    max = max, -- max weight
    cw = max, -- current weight
    peers = peers, -- peers
    name = backend.name
  }
  setmetatable(o, self)
  self.__index = self
  return o
end

function _M.balance(self)
  local peer, err = get_weighted_round_robin_peer(self)
  if not peer then
    return nil
  end

  return peer.name
end

function _M.sync(self, backend)
  local peers = build_peers(backend)
  local changed = not util.deep_compare(self.peers, peers)
  if not changed then
    return
  end

  local max = getmax(peers)
  local gcd = getgcd(peers)

  self.cp = 1 -- current peer index
  self.size = #peers -- peers count size
  self.gcd = gcd  -- GCD
  self.max = max -- max weight
  self.cw = max -- current weight
  self.peers = peers -- peers
end

return _M
