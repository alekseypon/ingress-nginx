local util = require("util")
local cjson = require("cjson.safe")


local _M = {}

function _M.new(self, o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function _M.sync(self, backend)
  local nodes
  self.traffic_shaping_policy = backend.trafficShapingPolicy
  self.alternative_backends = backend.alternativeBackends

  if backend.agent_checks_weights then
    nodes = util.get_nodes_weighted(backend.endpoints, backend.agent_checks_weights)
    -- ngx.log(ngx.ERR, string.format("get nodes for backend %s, worker id = %s, nodes=%s", cjson.encode(backend), ngx.worker.id(), cjson.encode(nodes)))
  else
    nodes = util.get_nodes(backend.endpoints, backend)
  end

  local changed = not util.deep_compare(self.instance.nodes, nodes)
  if not changed then
    return
  end

  ngx.log(ngx.ERR, string.format("reinit nodes for backend %s, worker id = %s, nodes=%s", backend.name, ngx.worker.id(), cjson.encode(nodes)))

  self.instance:reinit(nodes)
end

return _M
