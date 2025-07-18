// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package validation

import (
	"net/netip"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"

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
			},
		}
	})

	It("should return error if AddressRef.Name is empty", func() {
		ipClaim.Status.AddressRef.Name = ""
		errs := ValidateIPAddressClaim(ipClaim, metalNamespace, machineName)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("status").Child("addressRef").Child("name"), "IP address reference is required")))
	})

	It("should return error if labels are nil", func() {
		ipClaim.Labels = nil
		errs := ValidateIPAddressClaim(ipClaim, metalNamespace, machineName)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("labels"), "IP address claim labels are required")))
	})

	It("should return error if server claim name label is missing", func() {
		delete(ipClaim.Labels, LabelKeyServerClaimName)
		errs := ValidateIPAddressClaim(ipClaim, metalNamespace, machineName)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimName), "IP address claim has no server claim label for name")))
	})

	It("should return error if server claim namespace label is missing", func() {
		delete(ipClaim.Labels, LabelKeyServerClaimNamespace)
		errs := ValidateIPAddressClaim(ipClaim, metalNamespace, machineName)
		Expect(errs).To(ContainElement(field.Required(field.NewPath("metadata").Child("labels").Key(LabelKeyServerClaimNamespace), "IP address claim has no server claim label for namespace")))
	})

	It("should return error if labels do not match expected values", func() {
		ipClaim.Labels[LabelKeyServerClaimName] = "other"
		ipClaim.Labels[LabelKeyServerClaimNamespace] = "other-ns"
		errs := ValidateIPAddressClaim(ipClaim, metalNamespace, machineName)
		Expect(errs).To(ContainElement(field.Invalid(
			field.NewPath("metadata").Child("labels"),
			ipClaim.Labels,
			"IP address claim labels do not match expected values: ns/machine",
		)))
	})

	It("should not return error for valid claim", func() {
		errs := ValidateIPAddressClaim(ipClaim, metalNamespace, machineName)
		Expect(errs).To(BeEmpty())
	})
})
