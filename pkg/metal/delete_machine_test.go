// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"fmt"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("DeleteMachine", func() {
	ns, providerSecret, drv := SetupTest()

	It("should create and delete a machine", func(ctx SpecContext) {
		By("creating an metal machine")
		_, ipClaim1 := newIPRef("machine-0", ns.Name, "to-delete-pool-a", nil)
		_, ipClaim2 := newIPRef("machine-0", ns.Name, "to-delete-pool-b", nil)
		Expect(k8sClient.Create(ctx, ipClaim1)).To(Succeed())
		Expect(k8sClient.Create(ctx, ipClaim2)).To(Succeed())

		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   "machine-0",
		}))

		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      "machine-0",
			},
		}

		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      "machine-0",
			},
		}

		By("ensuring that the machine can be deleted")
		response, err := (*drv).DeleteMachine(ctx, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(response).To(Equal(&driver.DeleteMachineResponse{}))

		By("waiting for the machine to be gone")
		Eventually(Get(serverClaim)).Should(Satisfy(apierrors.IsNotFound))

		By("waiting for the ignition secret to be gone")
		Eventually(Get(ignition)).Should(Satisfy(apierrors.IsNotFound))

		Eventually(Get(ipClaim1)).Should(Satisfy(apierrors.IsNotFound))
		Eventually(Get(ipClaim2)).Should(Satisfy(apierrors.IsNotFound))
	})

	It("should create and delete a machine igntition secret created with old naming convention", func(ctx SpecContext) {
		By("creating an ignition secret")
		ignition := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      "machine-0",
			},
		}
		By("creating an metal machine")
		Expect((*drv).CreateMachine(ctx, &driver.CreateMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})).To(Equal(&driver.CreateMachineResponse{
			ProviderID: fmt.Sprintf("%s://%s/machine-%d", v1alpha1.ProviderName, ns.Name, 0),
			NodeName:   "machine-0",
		}))

		serverClaim := &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns.Name,
				Name:      "machine-0",
			},
		}

		By("ensuring that the machine can be deleted")
		response, err := (*drv).DeleteMachine(ctx, &driver.DeleteMachineRequest{
			Machine:      newMachine(ns, "machine", -1, nil),
			MachineClass: newMachineClass(v1alpha1.ProviderName, testing.SampleProviderSpec),
			Secret:       providerSecret,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(response).To(Equal(&driver.DeleteMachineResponse{}))

		By("waiting for the machine to be gone")
		Eventually(Get(serverClaim)).Should(Satisfy(apierrors.IsNotFound))

		By("waiting for the ignition secret to be gone")
		Eventually(Get(ignition)).Should(Satisfy(apierrors.IsNotFound))
	})
})
