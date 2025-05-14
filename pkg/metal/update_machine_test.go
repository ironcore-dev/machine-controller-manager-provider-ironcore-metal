// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"encoding/json"
	"fmt"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("UpdateMachine", func() {
	ns, providerSecret, drv := SetupTest()

	var server *metalv1alpha1.Server

	BeforeEach(func(ctx SpecContext) {
		By("creating a server")
		server = &metalv1alpha1.Server{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "server-",
				Labels: map[string]string{
					"instance-type": "machine-class",
				},
			},
			Spec: metalv1alpha1.ServerSpec{
				UUID:  "12345",
				Power: "On",
			},
		}
		Expect(k8sClient.Create(ctx, server)).To(Succeed())
		DeferCleanup(k8sClient.Delete, server)

		By("ensuring the server is in an available state")
		Eventually(UpdateStatus(server, func() {
			server.Status.State = metalv1alpha1.ServerStateAvailable
		})).Should(Succeed())
	})

	It("should create and update a machine", func(ctx SpecContext) {
		By("creating machine")
		machineName := "machine-0"
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
			HaveField("Spec.Image", "my-image"),
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

		By("update machine")
		Expect((*drv).UpdateMachine(ctx, &driver.UpdateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpecDiffOSImage),
			Secret:       providerSecret,
		})).To(Equal(&driver.UpdateMachineResponse{}))

		By("ensuring that the server claim has been updated")
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
			HaveField("Spec.Image", "new-image"),
		))

		By("failing if the machine request is empty")
		Eventually(func(g Gomega) {
			_, err := (*drv).UpdateMachine(ctx, nil)
			g.Expect(err.Error()).To(ContainSubstring("received empty request"))
		}).Should(Succeed())

		By("failing if the wrong provider is set")
		Eventually(func(g Gomega) {
			_, err := (*drv).UpdateMachine(ctx, &driver.UpdateMachineRequest{
				Machine:      newMachine(ns, "machine", -1, nil),
				MachineClass: newMachineClass("foo", testing.SampleProviderSpecDiffOSImage),
				Secret:       providerSecret,
			})
			g.Expect(err.Error()).To(ContainSubstring("not supported by the driver"))
		}).Should(Succeed())
	})
})
