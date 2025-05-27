// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("CreateMachine", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerClaimName)

	It("should create a machine", func(ctx SpecContext) {
		machineName := "machine-0"
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
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   machineName,
		}))

		By("ensuring that a server claim has been created")
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
			HaveField("Spec.Power", metalv1alpha1.PowerOn),
			HaveField("Spec.ServerSelector", &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"instance-type": "bar",
				},
			}),
		))

		By("ensuring that the ignition secret has been created")
		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		ignitionData, err := json.Marshal(testing.SampleIgnition)
		Expect(err).NotTo(HaveOccurred())
		Eventually(Object(ignition)).Should(SatisfyAll(
			HaveField("Data", HaveKeyWithValue("ignition", MatchJSON(ignitionData))),
		))
	})

	It("should create a machine with correct meta data", func(ctx SpecContext) {
		machineName := "machine-0"
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
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   machineName,
		}))

		By("ensuring that a server claim has been created")
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
			HaveField("Spec.Power", metalv1alpha1.PowerOn),
			HaveField("Spec.ServerSelector", &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"instance-type": "bar",
				},
			}),
		))

		By("ensuring that the ignition secret has been created")
		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      machineName,
			},
		}

		ignitionData, err := json.Marshal(testing.SampleIgnitionWithServerMetadata)
		Expect(err).NotTo(HaveOccurred())
		Eventually(Object(ignition)).Should(SatisfyAll(
			HaveField("Data", HaveKeyWithValue("ignition", MatchJSON(ignitionData))),
		))
	})

	It("should fail if the machine request is empty", func(ctx SpecContext) {
		By("failing if the machine request is empty")
		Eventually(func(g Gomega) {
			_, err := (*drv).CreateMachine(ctx, nil)
			g.Expect(err.Error()).To(ContainSubstring("received empty request"))
		}).Should(Succeed())
	})

	It("should fail if the machine request has a wrong provider", func(ctx SpecContext) {
		By("failing if the wrong provider is set")
		Eventually(func(g Gomega) {
			_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, "machine", -1, nil),
				MachineClass: newMachineClass("foo", testing.SampleProviderSpec),
				Secret:       providerSecret,
			})
			g.Expect(err.Error()).To(ContainSubstring("not supported by the driver"))
		}).Should(Succeed())
	})

	When("capi ipam references are present in ipamConfig", func() {
		It("should create ip claims and an ignition with ips", func(ctx SpecContext) {
			machineName := "machine-0"
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

			sampleProviderSpec := maps.Clone(testing.SampleProviderSpec)
			delete(sampleProviderSpec, "metaData")

			ipClaims := []*capiv1beta1.IPAddressClaim{}
			ips := []*capiv1beta1.IPAddress{}
			for _, pool := range []string{"pool-a", "pool-b"} {
				ip, ipClaim := newIPRef(machineName, ns.Name, pool, sampleProviderSpec)
				Expect(k8sClient.Create(ctx, ip)).To(Succeed())
				go func() {
					defer GinkgoRecover()
					Eventually(UpdateStatus(ipClaim, func() {
						ipClaim.Status.AddressRef.Name = ip.Name
					})).Should(Succeed())
				}()
				ipClaims = append(ipClaims, ipClaim)
				ips = append(ips, ip)
			}

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
			_, err := (*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
				Machine:      newMachine(ns, "machine", -1, nil),
				MachineClass: newMachineClass(v1alpha1.ProviderName, sampleProviderSpec),
				Secret:       providerSecret,
			})
			Expect(err).NotTo(HaveOccurred())

			By("ensuring that the server claim owns the ip address claims")
			ServerClaimKey := client.ObjectKey{Namespace: ns.Name, Name: machineName}
			ServerClaim := &metalv1alpha1.ServerClaim{}
			Eventually(k8sClient.Get(ctx, ServerClaimKey, ServerClaim)).Should(Succeed())

			for _, ipClaim := range ipClaims {
				Eventually(Object(ipClaim)).Should(SatisfyAll(
					HaveField("Labels", HaveKeyWithValue(LabelKeyServerClaimName, ServerClaim.Name)),
					HaveField("Labels", HaveKeyWithValue(LabelKeyServerClaimNamespace, ns.Name)),
					HaveField("OwnerReferences", ContainElement(
						metav1.OwnerReference{
							APIVersion: metalv1alpha1.GroupVersion.String(),
							Kind:       "ServerClaim",
							Name:       ServerClaim.Name,
							UID:        ServerClaim.UID,
						},
					))))
			}

			By("ensuring that the ignition secret has been created")
			ignition := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.Name,
					Name:      machineName,
				},
			}

			expected := base64.StdEncoding.EncodeToString([]byte(`{"pool-a":{"gateway":"10.11.12.1","ip":"10.11.12.13","prefix":24},"pool-b":{"gateway":"10.11.12.1","ip":"10.11.12.13","prefix":24}}`))
			Eventually(Object(ignition)).Should(SatisfyAll(
				WithTransform(func(sec *corev1.Secret) []interface{} {
					Expect(sec.Data).To(HaveKey("ignition"))
					var ignition map[string]interface{}
					Expect(json.Unmarshal(sec.Data["ignition"], &ignition)).To(Succeed())
					Expect(ignition).To(HaveKey("storage"))
					storage := ignition["storage"].(map[string]interface{})
					Expect(storage).To(HaveKey("files"))
					files := storage["files"].([]interface{})
					return files
				}, ContainElement(
					map[string]interface{}{
						"path": "/var/lib/metal-cloud-config/metadata",
						"contents": map[string]interface{}{
							"compression": "",
							"source":      "data:;base64," + expected,
						},
						"mode": 420.0,
					},
				)),
			))

			for _, ipClaim := range ipClaims {
				Expect(k8sClient.Delete(ctx, ipClaim)).To(Succeed())
			}
			for _, ip := range ips {
				Expect(k8sClient.Delete(ctx, ip)).To(Succeed())
			}
		})
	})
})

var _ = Describe("CreateMachine with Server name as hostname", func() {
	ns, providerSecret, drv := SetupTest(cmd.NodeNamePolicyServerName)

	It("should create a machine", func(ctx SpecContext) {
		machineName := "machine-0"
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
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   server.Name,
		}))

		By("ensuring that a server claim has been created")
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
			HaveField("Spec.Power", metalv1alpha1.PowerOn),
			HaveField("Spec.ServerSelector", &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"instance-type": "bar",
				},
			}),
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

		By("failing if the machine request is empty")
		Eventually(func(g Gomega) {
			_, err := (*drv).CreateMachine(ctx, nil)
			g.Expect(err.Error()).To(ContainSubstring("received empty request"))
		}).Should(Succeed())
	})
})
