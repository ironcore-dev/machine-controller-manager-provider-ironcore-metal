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
	machineNamePrefix := "machine-status"

	It("should create a machine and ensure status", func(ctx SpecContext) {
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

		By("check empty request")
		emptyMacReq := &driver.GetMachineStatusRequest{
			Machine:      nil,
			MachineClass: nil,
			Secret:       nil,
		}
		ret, err := (*drv).GetMachineStatus(ctx, emptyMacReq)
		Expect(ret).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, "received empty GetMachineStatusRequest")))

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
				serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
			})).Should(Succeed())
		}()

		By("creating machine")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
			NodeName:   machineName,
		}))

		By("ensuring the machine status")
		_, err = (*drv).GetMachineStatus(ctx, &driver.GetMachineStatusRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})

		Expect(err).To(HaveOccurred())
		Expect(err).Should(MatchError(status.Error(codes.Uninitialized, fmt.Sprintf("server claim %q is still not powered on, will reinitialize", machineName))))

		Eventually(func(g Gomega) {
			cmResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cmResponse).To(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   machineName,
			}))
		}).Should(Succeed())

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})

var _ = Describe("GetMachineStatus using Server names", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerName)
	machineNamePrefix := "machine-status"

	It("should create a machine and ensure status", func(ctx SpecContext) {
		machineIndex := 2
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
				serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
			})).Should(Succeed())
		}()

		By("creating machine")

		Eventually(func(g Gomega) {
			cmResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cmResponse).To(Equal(&driver.CreateMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   server.Name,
			}))
		}).Should(Succeed())

		By("ensuring the machine status")
		_, err := (*drv).GetMachineStatus(ctx, &driver.GetMachineStatusRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
		Expect(err).Should(MatchError(status.Error(codes.Uninitialized, fmt.Sprintf("server claim %q is still not powered on, will reinitialize", machineName))))

		Eventually(func(g Gomega) {
			cmResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cmResponse).To(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   server.Name,
			}))
		}).Should(Succeed())

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})

var _ = Describe("GetMachineStatus using BMC names", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyBMCName)
	machineNamePrefix := "machine-status"

	It("should create a machine and ensure status", func(ctx SpecContext) {
		By("creating a BMC")
		machineIndex := 3
		machineName := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex)
		bmc := &metalv1alpha1.BMC{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bmc-0",
			},
			Spec: metalv1alpha1.BMCSpec{
				Endpoint: &metalv1alpha1.InlineEndpoint{
					IP: metalv1alpha1.MustParseIP("127.0.0.1"),
				},
			},
		}
		Expect(k8sClient.Create(ctx, bmc)).To(Succeed())
		DeferCleanup(k8sClient.Delete, bmc)

		By("creating a server")
		server := &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-server",
			},
			Spec: metalv1alpha1.ServerSpec{
				SystemUUID: "12345",
				BMCRef: &corev1.LocalObjectReference{
					Name: bmc.Name,
				},
			},
		}
		Expect(k8sClient.Create(ctx, server)).To(Succeed())
		DeferCleanup(k8sClient.Delete, server)

		By("starting a non-blocking goroutine to patch ServerClaim")
		go func() {
			defer GinkgoRecover()
			serverClaim := &metalv1alpha1.ServerClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.Name,
					Name:      machineName,
				},
			}
			GinkgoWriter.Println("### Patching ServerClaim with ServerRef")
			Eventually(Update(serverClaim, func() {
				serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
			})).Should(Succeed())
			GinkgoWriter.Println("### ServerClaim updated with ServerRef")
		}()

		By("creating machine")
		Eventually(func(g Gomega) {
			cmResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cmResponse).To(Equal(&driver.CreateMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   bmc.Name,
			}))
		}).Should(Succeed())

		By("ensuring the machine status")
		_, err := (*drv).GetMachineStatus(ctx, &driver.GetMachineStatusRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
		Expect(err).Should(MatchError(status.Error(codes.Uninitialized, fmt.Sprintf("server claim %q is still not powered on, will reinitialize", machineName))))

		Eventually(func(g Gomega) {
			cmResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cmResponse).To(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   bmc.Name,
			}))
		}).Should(Succeed())

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})
