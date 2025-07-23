// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"fmt"
	"maps"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("CreateMachine", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerClaimName)
	machineNamePrefix := "machine-create"

	It("should create a machine", func(ctx SpecContext) {
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

		By("creating machine")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
			NodeName:   machineName,
		}))

		By("creating machine again to ensure idempotency")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
			NodeName:   machineName,
		}))

		By("ensuring that a ServerClaim has been created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: ns.Name,
			},
		}

		Eventually(Object(serverClaim)).Should(SatisfyAll(
			HaveField("ObjectMeta.Labels", map[string]string{
				ShootNameLabelKey:      "my-shoot",
				ShootNamespaceLabelKey: "my-shoot-namespace",
			}),
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
			HaveField("Spec.ServerSelector", &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"instance-type": "bar",
				},
			}),
		))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should create CAPI IPAddressClaims if ipamConfig is specified", func(ctx SpecContext) {
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

		providerSpec := maps.Clone(testing.SampleProviderSpec)

		ipClaims := []*capiv1beta1.IPAddressClaim{}
		for _, pool := range []string{"pool-a", "pool-b"} {
			ip, ipClaim := newIPRef(machineName, ns.Name, pool, providerSpec, "10.11.12.13", "10.11.12.1")

			Expect(k8sClient.Create(ctx, ip)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ip)

			ipClaims = append(ipClaims, ipClaim)
		}

		By("creating machine")
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())

		By("ensuring that the server claim owns the ip address claims")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}
		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		for _, ipClaim := range ipClaims {
			Eventually(Object(ipClaim)).Should(SatisfyAll(
				HaveField("ObjectMeta.Labels", map[string]string{
					validation.LabelKeyServerClaimName:      machineName,
					validation.LabelKeyServerClaimNamespace: ns.Name,
				}),
				HaveField("ObjectMeta.OwnerReferences", ContainElement(
					metav1.OwnerReference{
						APIVersion: metalv1alpha1.GroupVersion.String(),
						Kind:       "ServerClaim",
						Name:       serverClaim.Name,
						UID:        serverClaim.UID,
					},
				)),
				HaveField("Spec.PoolRef", BeElementOf([]corev1.TypedLocalObjectReference{
					{
						APIGroup: ptr.To("ipam.cluster.x-k8s.io"),
						Kind:     "GlobalInClusterIPPool",
						Name:     ipClaim.Name,
					},
				}),
				)))
			DeferCleanup(k8sClient.Delete, ipClaim)
		}

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should fail if the machine request is empty", func(ctx SpecContext) {
		By("failing if the machine request is empty")
		_, err := (*drv).CreateMachine(ctx, nil)
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, "received empty CreateMachineRequest")))
	})

	It("should fail if the machine request is not complete", func(ctx SpecContext) {
		By("failing if the machine request is not complete")
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: nil,
			Secret:       providerSecret,
		})
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, "received empty CreateMachineRequest")))
	})

	It("should fail if the machine request has a wrong provider", func(ctx SpecContext) {
		By("failing if the wrong provider is set")
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: newMachineClass("foo", testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, `requested provider "foo" is not supported by the driver "ironcore-metal"`)))
	})

	It("should fail if the provided secret do not contain userData", func(ctx SpecContext) {
		By("failing if the provided secret do not contain userData")
		notCompleteSecret := providerSecret.DeepCopy()
		notCompleteSecret.Data["userData"] = nil
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       notCompleteSecret,
		})
		Expect(err).Should(MatchError(status.Error(codes.Internal, `failed to get provider spec: failed to validate provider spec and secret: [userData: Required value: userData is required]`)))
	})

	It("should fail if the IPAM ref is not set", func(ctx SpecContext) {
		providerSpec := maps.Clone(testing.SampleProviderSpec)
		providerSpec["ipamConfig"] = []v1alpha1.IPAMConfig{
			{
				MetadataKey: "foo",
			},
		}

		By("failing if the IPAM ref is not set")
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
			Secret:       providerSecret,
		})
		Expect(err).Should(MatchError(status.Error(codes.Internal, `failed to create IPAddressClaims: machine codes error: code = [Internal] message = [IPAMRef of an IPAMConfig "foo" is not set]`)))
	})
})

var _ = Describe("CreateMachine with Server name as hostname", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerName)
	machineNamePrefix := "machine-create"

	It("should create a machine", func(ctx SpecContext) {
		machineIndex := 3
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

		By("ensuring that a ServerClaim has been created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: ns.Name,
			},
		}

		Eventually(Object(serverClaim)).Should(SatisfyAll(
			HaveField("ObjectMeta.Labels", map[string]string{
				ShootNameLabelKey:      "my-shoot",
				ShootNamespaceLabelKey: "my-shoot-namespace",
			}),
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
			HaveField("Spec.ServerSelector", &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"instance-type": "bar",
				},
			}),
		))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should fail if server not bound", func(ctx SpecContext) {
		machineIndex := 4
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

		By("creating machine")
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(status.Error(codes.Unavailable, fmt.Sprintf(`server %q in namespace %q is still not bound`, machineName, ns.Name))))

		By("ensuring that a ServerClaim has been created and has the recreate annotation")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: ns.Name,
			},
		}

		Eventually(Object(serverClaim)).Should(HaveField("ObjectMeta.Annotations", HaveKeyWithValue(validation.AnnotationKeyMCMMachineRecreate, "true")))

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

		By("ensuring that a server claim did not have recreate annotation after successful creation")
		Eventually(func(g Gomega) {
			_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		Eventually(Object(serverClaim)).ShouldNot(HaveField("ObjectMeta.Annotations", HaveKey(validation.AnnotationKeyMCMMachineRecreate)))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})

var _ = Describe("CreateMachine using BMC names", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyBMCName)
	machineNamePrefix := "machine-create"

	It("should fail if server not bound", func(ctx SpecContext) {
		machineIndex := 5
		machineName := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex)
		By("creating a BMC")
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

		By("creating machine")
		_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(status.Error(codes.Unavailable, fmt.Sprintf(`server %q in namespace %q is still not bound`, machineName, ns.Name))))

		By("ensuring that a ServerClaim has been created and has the recreate annotation")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: ns.Name,
			},
		}

		Eventually(Object(serverClaim)).Should(HaveField("ObjectMeta.Annotations", HaveKeyWithValue(validation.AnnotationKeyMCMMachineRecreate, "true")))

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

		By("ensuring that a server claim did not have recreate annotation after successful creation")
		Eventually(func(g Gomega) {
			_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		Eventually(Object(serverClaim)).ShouldNot(HaveField("ObjectMeta.Annotations", HaveKey(validation.AnnotationKeyMCMMachineRecreate)))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})
