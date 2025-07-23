// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package validation

import (
	"fmt"
	"net/netip"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
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
func ValidateIPAddressClaim(ipClaim *capiv1beta1.IPAddressClaim, serverClaim *metalv1alpha1.ServerClaim, serverClaimName, serverClaimNamespace string) field.ErrorList {
	var allErrs field.ErrorList

	if ipClaim.Labels == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels"), "IPAddressClaim labels are required"))
	}

	name, nameExists := ipClaim.Labels[LabelKeyServerClaimName]
	if !nameExists {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimName), "IPAddressClaim has no server claim label for name"))
	}

	if name != serverClaimName {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("metadata").Child("labels"),
			ipClaim.Labels,
			fmt.Sprintf("IPAddressClaim label %s do not match expected value: %s != %s", LabelKeyServerClaimName, name, serverClaimName),
		))
	}

	namespace, namespaceExists := ipClaim.Labels[LabelKeyServerClaimNamespace]
	if !namespaceExists {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimNamespace), "IPAddressClaim has no server claim label for namespace"))
	}

	if namespace != serverClaimNamespace {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("metadata").Child("labels"),
			ipClaim.Labels,
			fmt.Sprintf("IPAddressClaim label %s do not match expected value: %s != %s", LabelKeyServerClaimNamespace, namespace, serverClaimNamespace),
		))
	}

	if len(ipClaim.OwnerReferences) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("ownerReferences"), "IPAddressClaim must have an owner reference"))
	} else {
		for _, ownerRef := range ipClaim.OwnerReferences {
			if ownerRef.Kind != "ServerClaim" || ownerRef.Name != serverClaim.Name || ownerRef.APIVersion != metalv1alpha1.GroupVersion.String() {
				allErrs = append(allErrs, field.Invalid(
					field.NewPath("metadata").Child("ownerReferences"),
					ownerRef,
					fmt.Sprintf("IPAddressClaim owner reference does not match expected ServerClaim %s/%s", serverClaim.Namespace, serverClaim.Name),
				))
			}
		}
	}

	return allErrs
}
