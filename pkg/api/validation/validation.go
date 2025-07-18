// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package validation

import (
	"fmt"
	"net/netip"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
)

const (
	LabelKeyServerClaimName      = "metal.ironcore.dev/server-claim-name"
	LabelKeyServerClaimNamespace = "metal.ironcore.dev/server-claim-namespace"

	AnnotationKeyMCMMachineRecreate = "metal.ironcore.dev/mcm-machine-recreate"
)

// ValidateProviderSpecAndSecret validates the provider spec and provider secret
func ValidateProviderSpecAndSecret(spec *v1alpha1.ProviderSpec, secret *corev1.Secret, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	allErrs = validateMachineClassSpec(spec, field.NewPath("spec"))
	allErrs = append(allErrs, validateSecret(secret, field.NewPath("spec"))...)

	return allErrs
}

// validateSecret checks if the secret contains the required userData key
func validateSecret(secret *corev1.Secret, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if secret == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("secretRef"), "secretRef is required"))
		return allErrs
	}

	if secret.Data["userData"] == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("userData"), "userData is required"))
	}

	return allErrs
}

// validateMachineClassSpec validates if image is set and if DNS servers are valid IP addresses
func validateMachineClassSpec(spec *v1alpha1.ProviderSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if spec.Image == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("image"), "image is required"))
	}

	for i, ip := range spec.DnsServers {
		if !netip.Addr.IsValid(ip) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("dnsServers").Index(i), ip, "ip is invalid"))
		}
	}

	return allErrs
}

// ValidateIPAddressClaim validates the IPAddressClaim for a given machine
func ValidateIPAddressClaim(ipClaim *capiv1beta1.IPAddressClaim, metalNamespace, machineName string) field.ErrorList {
	var allErrs field.ErrorList

	if ipClaim.Status.AddressRef.Name == "" {
		allErrs = append(allErrs, field.Required(field.NewPath("status").Child("addressRef").Child("name"), "IP address reference is required"))
	}

	if ipClaim.Labels == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels"), "IP address claim labels are required"))
	}

	name, nameExists := ipClaim.Labels[LabelKeyServerClaimName]
	if !nameExists {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimName), "IP address claim has no server claim label for name"))
	}

	namespace, namespaceExists := ipClaim.Labels[LabelKeyServerClaimNamespace]
	if !namespaceExists {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimNamespace), "IP address claim has no server claim label for namespace"))
	}

	if name != machineName || namespace != metalNamespace {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("metadata").Child("labels"),
			ipClaim.Labels,
			fmt.Sprintf("IP address claim labels do not match expected values: %s/%s", metalNamespace, machineName),
		))
	}

	return allErrs
}
