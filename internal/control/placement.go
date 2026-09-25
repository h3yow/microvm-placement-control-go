package control

import (
	"fmt"
	"sort"

	"github.com/h3yow/microvm-placement-control-go/internal/model"
)

// Constraints 描述放置约束。
type Constraints struct {
	// RequiredCaps 为实例必需的后端能力。
	RequiredCaps []model.Capability
	// AffinityNodes 非空时仅允许放置到这些节点。
	AffinityNodes []string
	// AntiAffinityNodes 禁止放置到这些节点。
	AntiAffinityNodes []string
	// Spread 为 true 时优先选择剩余容量最多的节点（反亲和打散）。
	Spread bool
}

// PlacementDecision 记录放置判定依据，供审计与测试断言。
type PlacementDecision struct {
	NodeID   string
	Reason   string
	Rejected map[string]string // nodeID -> 拒绝原因
}

// chooseNode 在候选节点中按约束与容量选择节点；返回判定依据。
// 选择结果对相同输入稳定（按节点 ID 排序后扫描），保证可解释与可审计。
func chooseNode(nodes map[string]*model.Node, req model.Resources, c Constraints) (PlacementDecision, bool) {
	d := PlacementDecision{Rejected: map[string]string{}}

	aff := map[string]bool{}
	for _, id := range c.AffinityNodes {
		aff[id] = true
	}
	anti := map[string]bool{}
	for _, id := range c.AntiAffinityNodes {
		anti[id] = true
	}

	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	type cand struct {
		id    string
		avail model.Resources
	}
	var cands []cand
	for _, id := range ids {
		n := nodes[id]
		switch {
		case n.State != model.NodeReady:
			d.Rejected[id] = fmt.Sprintf("node state %s", n.State)
		case len(aff) > 0 && !aff[id]:
			d.Rejected[id] = "not in affinity set"
		case anti[id]:
			d.Rejected[id] = "excluded by anti-affinity"
		default:
			missing := ""
			for _, cap := range c.RequiredCaps {
				if !n.Caps[cap] {
					missing = fmt.Sprintf("missing capability %s", cap)
					break
				}
			}
			if missing != "" {
				d.Rejected[id] = missing
				continue
			}
			av := n.Available()
			if !av.Fits(req) {
				d.Rejected[id] = fmt.Sprintf("insufficient capacity: need %+v, avail %+v", req, av)
				continue
			}
			cands = append(cands, cand{id: id, avail: av})
		}
	}
	if len(cands) == 0 {
		d.Reason = "no suitable node"
		return d, false
	}
	best := cands[0]
	if c.Spread {
		for _, cd := range cands[1:] {
			if cd.avail.MilliCPU > best.avail.MilliCPU ||
				(cd.avail.MilliCPU == best.avail.MilliCPU && cd.avail.MemoryMB > best.avail.MemoryMB) {
				best = cd
			}
		}
		d.Reason = fmt.Sprintf("spread: node %s has most available %+v", best.id, best.avail)
	} else {
		d.Reason = fmt.Sprintf("first-fit (stable order): node %s avail %+v", best.id, best.avail)
	}
	d.NodeID = best.id
	return d, true
}
