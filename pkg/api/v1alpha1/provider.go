// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"net/netip"
)

const (
	// V1Alpha1 is the API version
	V1Alpha1 = "mcm.gardener.cloud/v1alpha1"
	// ProviderName is the provider name
	ProviderName = "ironcore-metal"
	// LoopbackAddressAnnotation is the annotation used to specify a loopback address for the Machine
	LoopbackAddressAnnotation = "metal.ironcore.dev/loopback-address"
)

// ProviderSpec is the spec to be used while parsing the calls
type ProviderSpec struct {
	// Image is the URL pointing to an OCI registry containing the operating system image which should be used to boot the Machine
	Image string `json:"image,omitempty"`
	// Ignition contains the ignition configuration which should be run on first boot of a Machine.
	Ignition string `json:"ignition,omitempty"`
	// By default, if ignition is set it will be merged it with our template
	// If IgnitionOverride is set to true allows to fully override
	IgnitionOverride bool `json:"ignitionOverride,omitempty"`
	// IgnitionSecretKey is optional key field used to identify the ignition content in the Secret
	// If the key is empty, the DefaultIgnitionKey will be used as fallback.
	IgnitionSecretKey string `json:"ignitionSecretKey,omitempty"`
	// Labels are used to tag resources which the MCM creates, so they can be identified later.
	Labels map[string]string `json:"labels,omitempty"`
	// DnsServers is a list of DNS resolvers which should be configured on the host.
	DnsServers []netip.Addr `json:"dnsServers,omitempty"`
	// ServerLabels are passed to the ServerClaim to find a server with certain properties
	ServerLabels map[string]string `json:"serverLabels,omitempty"`
	// Metadata is a key-value map of additional data which should be passed to the Machine.
	Metadata map[string]any `json:"metadata,omitempty"`
	// IPAMConfig is a list of references to Network resources that should be used to assign IP addresses to the worker nodes.
	IPAMConfig []IPAMConfig `json:"ipamConfig,omitempty"`
}

// IPAMObjectReference is a reference to the IPAM object, which will be used for IP allocation.
type IPAMObjectReference struct {
	// Name is the name of resource being referenced.
	Name string `json:"name"`
	// APIGroup is the group for the resource being referenced.
	APIGroup string `json:"apiGroup"`
	// Kind is the type of resource being referenced.
	Kind string `json:"kind"`
}

// IPAMConfig is a reference to an IPAM resource.
type IPAMConfig struct {
	// MetadataKey is the name of metadata key for the network.
	MetadataKey string `json:"metadataKey"`
	// IPAMRef is a reference to the IPAM object, which will be used for IP allocation.
	IPAMRef *IPAMObjectReference `json:"ipamRef"`
}
