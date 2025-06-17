// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package cmd

import "fmt"

type NodeNamePolicy string

const (
	NodeNamePolicyBMCName         NodeNamePolicy = "BMCName"
	NodeNamePolicyServerName      NodeNamePolicy = "ServerName"
	NodeNamePolicyServerClaimName NodeNamePolicy = "ServerClaimName"
)

// String returns the string representation of the NodeNamePolicy value
func (n *NodeNamePolicy) String() string {
	return string(*n)
}

func (n *NodeNamePolicy) Type() string {
	return string(*n)
}

// Set validates and sets the NodeNamePolicy value
func (n *NodeNamePolicy) Set(value string) error {
	switch NodeNamePolicy(value) {
	case NodeNamePolicyBMCName, NodeNamePolicyServerName, NodeNamePolicyServerClaimName:
		*n = NodeNamePolicy(value)
		return nil
	default:
		return fmt.Errorf("invalid NodeNamePolicy value: %s (must be '%s', '%s' or '%s')", value, NodeNamePolicyBMCName, NodeNamePolicyServerName, NodeNamePolicyServerClaimName)
	}
}
