// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"fmt"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("GetMachineStatus", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerClaimName)

	It("should create a machine and ensure status", func(ctx SpecContext) {
		By("check empty request")
		emptyMacReq := &driver.GetMachineStatusRequest{
			Machine:      nil,
			MachineClass: nil,
			Secret:       nil,
		}
		ret, err := (*drv).GetMachineStatus(ctx, emptyMacReq)
		Expect(ret).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, "received empty request")))

		By("starting a non-blocking goroutine to patch ServerClaim")
		go func() {
			defer GinkgoRecover()
			serverClaim := &metalv1alpha1.ServerClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.Name,
					Name:      "machine-0",
				},
			}
			Eventually(Update(serverClaim, func() {
				serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: "foo"}
			})).Should(Succeed())
		}()

		By("creating machine")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   "machine-0",
		}))

		By("ensuring the machine status")
		Expect((*drv).GetMachineStatus(ctx, &driver.GetMachineStatusRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.GetMachineStatusResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   "machine-0",
		}))
	})
})

var _ = Describe("GetMachineStatus using Server names", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerName)

	It("should create a machine and ensure status", func(ctx SpecContext) {
		machineName := "machine-0"

		By("starting a non-blocking goroutine to patch ServerClaim")
		go func() {
			defer GinkgoRecover()
			serverClaim := &metalv1alpha1.ServerClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.Name,
					Name:      machineName,
				},
			}
			Eventually(Update(serverClaim, func() {
				serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: "foo"}
			})).Should(Succeed())
		}()

		By("creating machine")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   "foo",
		}))

		By("ensuring the machine status")
		Expect((*drv).GetMachineStatus(ctx, &driver.GetMachineStatusRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.GetMachineStatusResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   "foo",
		}))
	})
})
