package config

import "gopkg.in/yaml.v3"

// The functions below write a Config back into the node tree it was loaded
// from, so that saving keeps the user's comments, key order and flow style.
// Only values change: keys that are new are appended, keys that are gone are dropped.

// mergeable reports whether a loaded tree can be updated in place. Anchors and
// aliases cannot: the decoded Config has them expanded.
func mergeable(n *yaml.Node) bool {
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return false
	}
	for _, c := range n.Content {
		if !mergeable(c) {
			return false
		}
	}
	return true
}

// mergeNode updates dst to hold src's value, keeping dst's comments and style.
func mergeNode(dst, src *yaml.Node) {
	if dst.Kind != src.Kind {
		head, line, foot := dst.HeadComment, dst.LineComment, dst.FootComment
		*dst = *src
		dst.HeadComment, dst.LineComment, dst.FootComment = head, line, foot
		return
	}
	switch dst.Kind {
	case yaml.ScalarNode:
		if dst.Value != src.Value || dst.Tag != src.Tag {
			dst.Value, dst.Tag = src.Value, src.Tag
			if dst.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0 {
				dst.Style = src.Style
			}
		}
	case yaml.MappingNode:
		mergeMapping(dst, src)
	case yaml.SequenceNode:
		mergeSequence(dst, src)
	}
}

// mergeMapping keeps dst's keys in their order, drops the ones src lacks and
// appends the ones only src has.
func mergeMapping(dst, src *yaml.Node) {
	srcIdx := make(map[string]int, len(src.Content)/2)
	for i := 0; i+1 < len(src.Content); i += 2 {
		srcIdx[src.Content[i].Value] = i
	}
	out := make([]*yaml.Node, 0, len(src.Content))
	kept := make(map[string]bool, len(srcIdx))
	for i := 0; i+1 < len(dst.Content); i += 2 {
		key := dst.Content[i].Value
		j, ok := srcIdx[key]
		if !ok || kept[key] {
			continue
		}
		mergeNode(dst.Content[i+1], src.Content[j+1])
		out = append(out, dst.Content[i], dst.Content[i+1])
		kept[key] = true
	}
	for i := 0; i+1 < len(src.Content); i += 2 {
		if !kept[src.Content[i].Value] {
			out = append(out, src.Content[i], src.Content[i+1])
		}
	}
	dst.Content = out
}

// mergeSequence follows src's order. An element reuses the dst element with the
// same "name" (contexts), or failing that the unclaimed one at the same index,
// so renaming a context still keeps its comments.
func mergeSequence(dst, src *yaml.Node) {
	byName := make(map[string]int)
	for i, n := range dst.Content {
		if name := nameOf(n); name != "" {
			byName[name] = i
		}
	}
	match := make([]int, len(src.Content))
	claimed := make([]bool, len(dst.Content))
	for i, n := range src.Content {
		match[i] = -1
		if j, ok := byName[nameOf(n)]; ok && !claimed[j] {
			match[i], claimed[j] = j, true
		}
	}
	for i := range src.Content {
		if match[i] < 0 && i < len(dst.Content) && !claimed[i] {
			match[i], claimed[i] = i, true
		}
	}
	out := make([]*yaml.Node, len(src.Content))
	for i, n := range src.Content {
		if match[i] < 0 {
			out[i] = n
			continue
		}
		mergeNode(dst.Content[match[i]], n)
		out[i] = dst.Content[match[i]]
	}
	dst.Content = out
}

// nameOf returns the value of a mapping's "name" key, or "".
func nameOf(n *yaml.Node) string {
	if n.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == "name" && n.Content[i+1].Kind == yaml.ScalarNode {
			return n.Content[i+1].Value
		}
	}
	return ""
}
