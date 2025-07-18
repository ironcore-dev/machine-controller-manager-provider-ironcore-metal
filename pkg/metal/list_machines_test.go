// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"fmt"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ListMachines", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerClaimName)
	machineNamePrefix := "machine-list"

	It("should fail if no provider has been set", func(ctx SpecContext) {
		By("ensuring an error if no provider has been set")
		_, err := (*drv).ListMachines(ctx, &driver.ListMachinesRequest{
			MachineClass: newMachineClass("", testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
	})

	It("should list no machines if none have been created", func(ctx SpecContext) {
		By("ensuring the list response contains no machines")
		listMachineResponse, err := (*drv).ListMachines(ctx, &driver.ListMachinesRequest{
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(listMachineResponse.MachineList).To(Equal(map[string]string{}))
	})

	It("should list a single machine if one has been created", func(ctx SpecContext) {
		machineIndex := 1
		machineName := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex)
		By("creating a server")
		server := &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-server",
			},
			Spec: metalv1alpha1.ServerSpec{
				SystemUUID: "12345",
			},
		}
		Expect(k8sClient.Create(ctx, server)).To(Succeed())
		DeferCleanup(k8sClient.Delete, server)

		By("creating a machine")
		craeteMachineResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(craeteMachineResponse).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
			NodeName:   machineName,
		}))

		By("ensuring the list response contains the correct machine")
		listMachineResponse, err := (*drv).ListMachines(ctx, &driver.ListMachinesRequest{
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(listMachineResponse.MachineList).To(Equal(
			map[string]string{fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex): machineName},
		))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should list two machines if two have been created", func(ctx SpecContext) {
		machineIndex := 2
		machineName := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex)
		machineIndex2 := 3
		machineName2 := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex2)
		By("creating a server")
		server := &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-server",
			},
			Spec: metalv1alpha1.ServerSpec{
				SystemUUID: "12345",
			},
		}
		Expect(k8sClient.Create(ctx, server)).To(Succeed())
		DeferCleanup(k8sClient.Delete, server)

		By("creating a server")
		server2 := &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-server-2",
			},
			Spec: metalv1alpha1.ServerSpec{
				SystemUUID: "12345",
			},
		}
		Expect(k8sClient.Create(ctx, server2)).To(Succeed())
		DeferCleanup(k8sClient.Delete, server2)

		By("creating the first machine")
		craeteMachineResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(craeteMachineResponse).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
			NodeName:   machineName,
		}))

		By("creating the second machine")
		craeteMachineResponse, err = (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex2, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(craeteMachineResponse).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex2),
			NodeName:   machineName2,
		}))

		By("ensuring the machine status contains 2 machines")
		listMachinesResponse, err := (*drv).ListMachines(ctx, &driver.ListMachinesRequest{
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(listMachinesResponse.MachineList).To(Equal(map[string]string{
			fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex):  machineName,
			fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex2): machineName2,
		}))

		By("ensuring the cleanup of the first machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})

		By("ensuring the cleanup of the second machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex2, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})
