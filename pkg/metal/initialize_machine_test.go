// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"encoding/base64"
	"encoding/json"
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

var _ = Describe("InitializeMachine", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerClaimName)
	machineNamePrefix := "machine-init"

	It("should create and initialize a machine", func(ctx SpecContext) {
		machineIndex := 1
		machineName := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex)
		By("creating a server")
		server := &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-server",
				Annotations: map[string]string{
					v1alpha1.LoopbackAddressAnnotation: "2001:db8::1",
				},
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

		By("ensuring that a ServerClaim has been created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: ns.Name,
			},
		}

		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		By("failing on initial initialization of the machine, ServerClaim still not bound")
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).To(MatchError(status.Error(codes.Internal, fmt.Sprintf(`ServerClaim %s/%s still not bound`, ns.Name, machineName))))

		By("patching ServerClaim with ServerRef")
		Eventually(Update(serverClaim, func() {
			serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
		})).Should(Succeed())

		By("retrying initialization of the machine")
		Eventually(func(g Gomega) {
			g.Expect((*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})).Should(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   machineName,
			}))
		}).Should(Succeed())

		By("ensuring that the ignition secret has been created")
		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		ignitionMetadata := testing.SampleIgnitionWithServerMetadata
		ignitionMetadata["storage"].(map[string]any)["files"].([]any)[0].(map[string]any)["contents"].(map[string]any)["source"] = fmt.Sprintf("data:,machine-init-%d%%0A", machineIndex)
		ignitionData, err := json.Marshal(ignitionMetadata)

		Expect(err).NotTo(HaveOccurred())
		Eventually(Object(ignition)).Should(SatisfyAll(
			HaveField("Data", HaveKeyWithValue("ignition", MatchJSON(ignitionData))),
		))

		By("ensuring that the ignition secret is referenced in ServerClaim and power is set to PowerOn")
		Eventually(Object(serverClaim)).Should(SatisfyAll(
			HaveField("Spec.Power", metalv1alpha1.PowerOn),
			HaveField("Spec.IgnitionSecretRef.Name", machineName),
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

			go func() {
				defer GinkgoRecover()
				Eventually(UpdateStatus(ipClaim, func() {
					ipClaim.Status.AddressRef.Name = ip.Name
				})).Should(Succeed())
			}()
		}

		By("creating machine")
		createMachineResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(createMachineResponse).ToNot(BeNil())
		Expect(createMachineResponse.ProviderID).To(Equal(fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex)))
		Expect(createMachineResponse.NodeName).To(Equal(machineName))

		By("ensuring that the ServerClaim is created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}
		Eventually(Get(serverClaim)).Should(Succeed())

		By("patching ServerClaim with ServerRef")
		Eventually(Update(serverClaim, func() {
			serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
		})).Should(Succeed())

		By("initialization of the machine")
		Eventually(func(g Gomega) {
			g.Expect((*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
				Secret:       providerSecret,
			})).Should(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   machineName,
			}))
		}).Should(Succeed())

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

	It("should create ingnition configured when there is predefined IPAM config with IPAddressClaims and IPs", func(ctx SpecContext) {
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
		delete(providerSpec, "metaData")

		for _, pool := range []string{"pool-c", "pool-d"} {
			ip, ipClaim := newIPRef(machineName, ns.Name, pool, providerSpec, "10.11.13.13", "10.11.13.1")
			Expect(k8sClient.Create(ctx, ip)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ip)

			By("starting a non-blocking goroutine to patch IPAddressClaim")
			go func() {
				defer GinkgoRecover()
				Eventually(UpdateStatus(ipClaim, func() {
					ipClaim.Status.AddressRef.Name = ip.Name
				})).Should(Succeed())
			}()
		}

		By("creating machine")
		createMachineResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(createMachineResponse).ToNot(BeNil())
		Expect(createMachineResponse.ProviderID).To(Equal(fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex)))
		Expect(createMachineResponse.NodeName).To(Equal(machineName))

		By("ensuring that a ServerClaim has been created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		By("patching ServerClaim with ServerRef")
		Eventually(Update(serverClaim, func() {
			serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
		})).Should(Succeed())

		By("initialization of the machine")
		Eventually(func(g Gomega) {
			g.Expect((*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
				Secret:       providerSecret,
			})).Should(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   machineName,
			}))
		}).Should(Succeed())

		By("ensuring that the ignition secret has been created")
		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		expected := base64.StdEncoding.EncodeToString([]byte(`{"pool-c":{"gateway":"10.11.13.1","ip":"10.11.13.13","prefix":24},"pool-d":{"gateway":"10.11.13.1","ip":"10.11.13.13","prefix":24}}`))
		Eventually(Object(ignition)).Should(SatisfyAll(
			WithTransform(func(sec *corev1.Secret) []any {
				Expect(sec.Data).To(HaveKey("ignition"))
				var ignition map[string]any
				Expect(json.Unmarshal(sec.Data["ignition"], &ignition)).To(Succeed())
				Expect(ignition).To(HaveKey("storage"))
				storage := ignition["storage"].(map[string]any)
				Expect(storage).To(HaveKey("files"))
				files := storage["files"].([]any)
				return files
			}, ContainElement(
				map[string]any{
					"path": "/var/lib/metal-cloud-config/metadata",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:;base64," + expected,
					},
					"mode": 420.0,
				},
			)),
		))

		By("ensuring that the ignition secret is referenced in ServerClaim and power is set to PowerOn")
		Eventually(Object(serverClaim)).Should(SatisfyAll(
			HaveField("Spec.Power", metalv1alpha1.PowerOn),
			HaveField("Spec.IgnitionSecretRef.Name", machineName),
		))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should fail if the machine request is empty", func(ctx SpecContext) {
		By("failing if the machine request is empty")
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, nil)
		Expect(err).Should(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, "received empty InitializeMachineRequest")))
	})

	It("should fail if the machine request is not complete", func(ctx SpecContext) {
		By("failing if the machine request is not complete")
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: nil,
			Secret:       providerSecret,
		})
		Expect(err).Should(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, "received empty InitializeMachineRequest")))
	})

	It("should fail if the machine request has a wrong provider", func(ctx SpecContext) {
		By("failing if the wrong provider is set")
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: newMachineClass("foo", testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).Should(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.InvalidArgument, `requested provider "foo" is not supported by the driver "ironcore-metal"`)))
	})

	It("should fail if the provided secret do not contain userData", func(ctx SpecContext) {
		By("failing if the provided secret do not contain userData")
		notCompleteSecret := providerSecret.DeepCopy()
		notCompleteSecret.Data["userData"] = nil
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       notCompleteSecret,
		})
		Expect(err).Should(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.Internal, `failed to get provider spec: failed to validate provider spec and secret: [userData: Required value: userData is required]`)))
	})

	It("should fail initialization when ServerClaim still not bound", func(ctx SpecContext) {
		machineIndex := 4
		machineName := fmt.Sprintf("%s-%d", machineNamePrefix, machineIndex)
		By("creating a server")
		server := &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-server",
				Annotations: map[string]string{
					v1alpha1.LoopbackAddressAnnotation: "2001:db8::1",
				},
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

		By("ensuring that a ServerClaim has been created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      machineName,
				Namespace: ns.Name,
			},
		}

		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		By("failing on initial initialization of the  machine, ServerClaim still not bound")
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).To(MatchError(status.Error(codes.Internal, fmt.Sprintf(`ServerClaim %s/%s still not bound`, ns.Name, machineName))))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should fail initialization when IPAddressClaim still not bound", func(ctx SpecContext) {
		machineIndex := 5
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
		delete(providerSpec, "metaData")

		poolName := "pool-a"
		_, ipClaim := newIPRef(machineName, ns.Name, poolName, providerSpec, "10.11.14.13", "10.11.14.1")

		By("creating machine")
		createMachineResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(createMachineResponse).ToNot(BeNil())
		Expect(createMachineResponse.ProviderID).To(Equal(fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex)))
		Expect(createMachineResponse.NodeName).To(Equal(machineName))

		By("ensuring that a ServerClaim has been created")
		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		By("patching ServerClaim with ServerRef")
		Eventually(Update(serverClaim, func() {
			serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
		})).Should(Succeed())

		By("initialization of the machine")
		Eventually(func(g Gomega) {
			initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).To(HaveOccurred())
			g.Expect(initializeMachineResponse).To(BeNil())
			g.Expect(err).To(MatchError(status.Error(codes.Internal, fmt.Sprintf("failed to collect IPAddress metadata: IPAddressClaim %s/%s-%s not bound", ns.Name, machineName, poolName))))
		}).Should(Succeed())

		DeferCleanup(k8sClient.Delete, ipClaim)

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})

	It("should fail if the IPAM ref is not set", func(ctx SpecContext) {
		machineIndex := 6
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
		providerSpec["ipamConfig"] = []v1alpha1.IPAMConfig{
			{
				MetadataKey: "foo",
			},
		}

		By("creating machine")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
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

		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		By("patching ServerClaim with ServerRef")
		Eventually(Update(serverClaim, func() {
			serverClaim.Spec.ServerRef = &corev1.LocalObjectReference{Name: server.Name}
		})).Should(Succeed())

		By("failing if the IPAM ref is not set")
		initializeMachineResponse, err := (*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, providerSpec),
			Secret:       providerSecret,
		})
		Expect(err).To(HaveOccurred())
		Expect(initializeMachineResponse).To(BeNil())
		Expect(err).Should(MatchError(status.Error(codes.Internal, `failed to create IPAddressClaims: machine codes error: code = [Internal] message = [IPAMRef of an IPAMConfig "foo" is not set]`)))
	})
})

var _ = Describe("InitializeMachine with Server name as hostname", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerName)
	machineNamePrefix := "machine-init"

	It("should create and initialize a machine", func(ctx SpecContext) {
		machineIndex := 7
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
			createMachineResponse, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(createMachineResponse).To(Equal(&driver.CreateMachineResponse{
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

		Eventually(Object(serverClaim)).Should(
			HaveField("Spec.Power", metalv1alpha1.PowerOff),
		)

		By("initializing machine")
		Eventually(func(g Gomega) {
			g.Expect((*drv).InitializeMachine(ctx, &driver.InitializeMachineRequest{
				Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
				Secret:       providerSecret,
			})).Should(Equal(&driver.InitializeMachineResponse{
				ProviderID: fmt.Sprintf("%s://%s/%s-%d", v1alpha1.ProviderName, ns.Name, machineNamePrefix, machineIndex),
				NodeName:   server.Name,
			}))
		}).Should(Succeed())

		Eventually(Object(serverClaim)).Should(SatisfyAll(
			HaveField("Spec.Power", metalv1alpha1.PowerOn),
		))

		By("ensuring that the ignition secret has been created")
		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		ignitionData, err := json.Marshal(testing.SampleIgnitionWithTestServerHostname)
		Expect(err).NotTo(HaveOccurred())
		Eventually(Object(ignition)).Should(SatisfyAll(
			HaveField("Data", HaveKeyWithValue("ignition", MatchJSON(ignitionData))),
		))

		By("ensuring the cleanup of the machine")
		DeferCleanup((*drv).DeleteMachine, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, machineNamePrefix, machineIndex, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
	})
})
