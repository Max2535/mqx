package tui

import "github.com/Max2535/mqx/internal/broker"

// deepLinks are the Options that select a topic or group on start.
type deepLinks struct {
	topic, group string
}

// panelSpec builds a panel when the broker has its capability ("" = core).
type panelSpec struct {
	capability string
	build      func(e *env, links deepLinks) panel
}

// panelSpecs lists every panel in navigation order. Adding a capability
// panel means adding one entry here.
func panelSpecs() []panelSpec {
	return []panelSpec{
		{"", func(e *env, l deepLinks) panel { return newTopicsPanel(e, l.topic) }},
		{broker.CapConsumerInspector, func(e *env, _ deepLinks) panel { return newConsumersPanel(e) }},
		{broker.CapGroupInspector, func(e *env, l deepLinks) panel { return newGroupsPanel(e, l.group) }},
		{broker.CapTopologyInspector, func(e *env, _ deepLinks) panel { return newTopologyPanel(e) }},
		{broker.CapConnectionInspector, func(e *env, _ deepLinks) panel { return newConnectionsPanel(e) }},
		{broker.CapClusterInspector, func(e *env, _ deepLinks) panel { return newClusterPanel(e) }},
		{broker.CapSchemaRegistry, func(e *env, _ deepLinks) panel { return newSchemaPanel(e) }},
		{broker.CapConnectManager, func(e *env, _ deepLinks) panel { return newConnectPanel(e) }},
		{broker.CapKSQLRunner, func(e *env, _ deepLinks) panel { return newKSQLPanel(e) }},
		{broker.CapACLAdmin, func(e *env, _ deepLinks) panel { return newACLPanel(e) }},
		{broker.CapUserAdmin, func(e *env, _ deepLinks) panel { return newUsersPanel(e) }},
		{broker.CapPolicyAdmin, func(e *env, _ deepLinks) panel { return newPoliciesPanel(e) }},
		{broker.CapMetricsReporter, func(e *env, _ deepLinks) panel { return newMetricsPanel(e) }},
	}
}

// buildPanels returns only the panels the broker supports.
func buildPanels(e *env, links deepLinks) []panel {
	var out []panel
	for _, s := range panelSpecs() {
		if s.capability == "" || e.has(s.capability) {
			out = append(out, s.build(e, links))
		}
	}
	return out
}
