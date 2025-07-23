// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package validation

import (
	"fmt"
	"net/netip"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
)

var fldPath *field.Path

var _ = Describe("Machine", func() {
	invalidIP := netip.Addr{}

	DescribeTable("ValidateProviderSpecAndSecret",
		func(spec *v1alpha1.ProviderSpec, secret *corev1.Secret, fldPath *field.Path, match types.GomegaMatcher) {
			errList := ValidateProviderSpecAndSecret(spec, secret, fldPath)
			Expect(errList).To(match)
		},
		Entry("no secret",
			&v1alpha1.ProviderSpec{},
			nil,
			fldPath,
			ContainElement(field.Required(fldPath.Child("spec.secretRef"), "secretRef is required")),
		),
		Entry("no userData in secret",
			&v1alpha1.ProviderSpec{},
			&corev1.Secret{
				Data: map[string][]byte{
					"userData": nil,
				},
			},
			fldPath,
			ContainElement(field.Required(fldPath.Child("userData"), "userData is required")),
		),
		Entry("no image",
			&v1alpha1.ProviderSpec{
				Image: "",
			},
			&corev1.Secret{},
			fldPath,
			ContainElement(field.Required(fldPath.Child("spec.image"), "image is required")),
		),
		Entry("invalid dns server ip",
			&v1alpha1.ProviderSpec{
				DnsServers: []netip.Addr{invalidIP},
			},
			&corev1.Secret{},
			fldPath,
			ContainElement(field.Invalid(fldPath.Child("spec.dnsServers[0]"), invalidIP, "ip is invalid")),
		),
	)
})

var _ = Describe("validateSecret", func() {
	It("should return error if secret is nil", func() {
		errs := validateSecret(nil, field.NewPath("spec"))
		Expect(errs).To(ContainElement(field.Required(field.NewPath("spec.secretRef"), "secretRef is required")))
	})

	It("should return error if userData is missing", func() {
		secret := &corev1.Secret{Data: map[string][]byte{}}
		errs := validateSecret(secret, field.NewPath("spec"))
		Expect(errs).To(ContainElement(field.Required(field.NewPath("userData"), "userData is required")))
	})

	It("should not return error if userData is present", func() {
		secret := &corev1.Secret{Data: map[string][]byte{"userData": []byte("data")}}
		errs := validateSecret(secret, field.NewPath("spec"))
		Expect(errs).To(BeEmpty())
	})
})

var _ = Describe("validateMachineClassSpec", func() {
	It("should return error if image is empty", func() {
		spec := &v1alpha1.ProviderSpec{Image: ""}
		errs := validateMachineClassSpec(spec, field.NewPath("spec"))
		Expect(errs).To(ContainElement(field.Required(field.NewPath("spec.image"), "image is required")))
	})

	It("should return error for invalid dnsServers", func() {
		spec := &v1alpha1.ProviderSpec{Image: "img", DnsServers: []netip.Addr{{}}}
		errs := validateMachineClassSpec(spec, field.NewPath("spec"))
		Expect(errs).To(ContainElement(field.Invalid(field.NewPath("spec.dnsServers").Index(0), netip.Addr{}, "ip is invalid")))
	})

	It("should not return error for valid image and dnsServers", func() {
		addr := netip.MustParseAddr("8.8.8.8")
		spec := &v1alpha1.ProviderSpec{Image: "img", DnsServers: []netip.Addr{addr}}
		errs := validateMachineClassSpec(spec, field.NewPath("spec"))
		Expect(errs).To(BeEmpty())
	})
})

var _ = Describe("ValidateIPAddressClaim", func() {
	var (
		ipClaim        *capiv1beta1.IPAddressClaim
		serverClaim    *metalv1alpha1.ServerClaim
		metalNamespace = "ns"
		machineName    = "machine"
	)

	BeforeEach(func() {
		ipClaim = &capiv1beta1.IPAddressClaim{
			Status: capiv1beta1.IPAddressClaimStatus{
				AddressRef: corev1.LocalObjectReference{Name: "ipref"},
			},
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					LabelKeyServerClaimName:      machineName,
					LabelKeyServerClaimNamespace: metalNamespace,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						Kind:       "ServerClaim",
						Name:       machineName,
						APIVersion: metalv1alpha1.GroupVersion.String(),
					},
				},
			},
		}
		serverClaim = &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: metalNamespace,
			},
			Spec: metalv1alpha1.ServerClaimSpec{
				Power: metalv1alpha1.PowerOn,
				ServerSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"server": "label"},
				},
				Image: "image",
			},
		}
	})

	It("should return error if labels are nil", func() {
		ipClaim.Labels = nil
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("labels"), "IPAddressClaim labels are required")))
	})

	It("should return error if server claim name label is missing", func() {
		delete(ipClaim.Labels, LabelKeyServerClaimName)
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimName), "IPAddressClaim has no server claim label for name")))
	})

	It("should return error if server claim namespace label is missing", func() {
		delete(ipClaim.Labels, LabelKeyServerClaimNamespace)
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimNamespace), "IPAddressClaim has no server claim label for namespace")))
	})

	It("should return error if labels server-claim-name do not match expected value", func() {
		ipClaim.Labels[LabelKeyServerClaimName] = "other"
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Invalid(
			field.NewPath("metadata").Child("labels"),
			ipClaim.Labels,
			"IPAddressClaim label metal.ironcore.dev/server-claim-name do not match expected value: other != machine",
		)))
	})

	It("should return error if labels server-claim-namespace do not match expected value", func() {
		ipClaim.Labels[LabelKeyServerClaimNamespace] = "other-ns"
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Invalid(
			field.NewPath("metadata").Child("labels"),
			ipClaim.Labels,
			"IPAddressClaim label metal.ironcore.dev/server-claim-namespace do not match expected value: other-ns != ns",
		)))
	})

	It("should not return error for valid claim", func() {
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(BeEmpty())
	})

	It("should return error if ownerReferences are empty", func() {
		ipClaim.OwnerReferences = nil
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("ownerReferences"), "IPAddressClaim must have an owner reference")))
	})

	It("should return error if ownerReference kind is invalid", func() {
		ipClaim.OwnerReferences = []metav1.OwnerReference{
			{
				Kind:       "InvalidKind",
				Name:       serverClaim.Name,
				APIVersion: metalv1alpha1.GroupVersion.String(),
			},
		}
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Invalid(
			field.NewPath("metadata").Child("ownerReferences"),
			ipClaim.OwnerReferences[0],
			fmt.Sprintf("IPAddressClaim owner reference does not match expected ServerClaim %s/%s", serverClaim.Namespace, serverClaim.Name),
		)))
	})

	It("should return error if ownerReference name is invalid", func() {
		ipClaim.OwnerReferences = []metav1.OwnerReference{
			{
				Kind:       "ServerClaim",
				Name:       "InvalidName",
				APIVersion: metalv1alpha1.GroupVersion.String(),
			},
		}
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Invalid(
			field.NewPath("metadata").Child("ownerReferences"),
			ipClaim.OwnerReferences[0],
			fmt.Sprintf("IPAddressClaim owner reference does not match expected ServerClaim %s/%s", serverClaim.Namespace, serverClaim.Name),
		)))
	})

	It("should return error if ownerReference APIVersion is invalid", func() {
		ipClaim.OwnerReferences = []metav1.OwnerReference{
			{
				Kind:       "ServerClaim",
				Name:       serverClaim.Name,
				APIVersion: "invalid.api.version",
			},
		}
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(ContainElement(field.Invalid(
			field.NewPath("metadata").Child("ownerReferences"),
			ipClaim.OwnerReferences[0],
			fmt.Sprintf("IPAddressClaim owner reference does not match expected ServerClaim %s/%s", serverClaim.Namespace, serverClaim.Name),
		)))
	})

	It("should not return error if ownerReferences are valid", func() {
		ipClaim.OwnerReferences = []metav1.OwnerReference{
			{
				Kind:       "ServerClaim",
				Name:       serverClaim.Name,
				APIVersion: metalv1alpha1.GroupVersion.String(),
			},
		}
		errs := ValidateIPAddressClaim(ipClaim, serverClaim, machineName, metalNamespace)
		Expect(errs).To(BeEmpty())
	})
})
