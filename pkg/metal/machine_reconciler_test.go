// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"encoding/json"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal/testing"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("MachineReconciler", func() {
	ns, _, _ := SetupTest(cmd.NodeNamePolicyServerClaimName)

	newMachineClassWithImage := func(ns, name, image string) *machinev1alpha1.MachineClass {
		spec := map[string]any{}
		for k, v := range testing.SampleProviderSpec {
			spec[k] = v
		}
		spec["image"] = image
		providerSpecJSON, err := json.Marshal(spec)
		Expect(err).NotTo(HaveOccurred())
		return &machinev1alpha1.MachineClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
			},
			ProviderSpec: kuberuntime.RawExtension{Raw: providerSpecJSON},
			Provider:     v1alpha1.ProviderName,
		}
	}

	newMachineSet := func(ns, name, machineClassName string) *machinev1alpha1.MachineSet {
		return &machinev1alpha1.MachineSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
			},
			Spec: machinev1alpha1.MachineSetSpec{
				MachineClass: machinev1alpha1.ClassSpec{
					Name: machineClassName,
					Kind: "MachineClass",
				},
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"machineset": name},
				},
			},
		}
	}

	newMachineWithUpdateLabel := func(ns, name, machineSetName string, machineSetUID types.UID) *machinev1alpha1.Machine {
		return &machinev1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				Labels: map[string]string{
					machinev1alpha1.LabelKeyNodeUpdateResult: machinev1alpha1.LabelValueNodeUpdateSuccessful,
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "machine.sapcloud.io/v1alpha1",
						Kind:       "MachineSet",
						Name:       machineSetName,
						UID:        machineSetUID,
						Controller: ptr.To(true),
					},
				},
			},
		}
	}

	newServerClaim := func(ns, name, image string) *metalv1alpha1.ServerClaim {
		return &metalv1alpha1.ServerClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
			},
			Spec: metalv1alpha1.ServerClaimSpec{
				Image: image,
				Power: metalv1alpha1.PowerOn,
			},
		}
	}

	reconcileRequest := func(ns, machineName string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: machineName}}
	}

	It("should update ServerClaim image when Machine has update-result=successful label", func(ctx SpecContext) {
		const (
			oldImage = "my-image"
			newImage = "new-image"
		)

		By("creating a MachineClass with the new image")
		machineClass := newMachineClassWithImage(ns.Name, "new-machine-class", newImage)
		Expect(k8sClient.Create(ctx, machineClass)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineClass)

		By("creating a MachineSet referencing the new MachineClass")
		machineSet := newMachineSet(ns.Name, "new-machine-set", machineClass.Name)
		Expect(k8sClient.Create(ctx, machineSet)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineSet)

		By("creating a Machine with update-result=successful label owned by the new MachineSet")
		machine := newMachineWithUpdateLabel(ns.Name, "machine-0", machineSet.Name, machineSet.UID)
		Expect(k8sClient.Create(ctx, machine)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machine)

		By("creating a ServerClaim with the old image")
		serverClaim := newServerClaim(ns.Name, machine.Name, oldImage)
		Expect(k8sClient.Create(ctx, serverClaim)).To(Succeed())
		DeferCleanup(k8sClient.Delete, serverClaim)

		By("running the reconciler")
		metalClientProvider := &mcmclient.Provider{}
		metalClientProvider.SetClient(k8sClient)
		reconciler := NewMachineReconciler(k8sClient, metalClientProvider, ns.Name)
		_, err := reconciler.Reconcile(ctx, reconcileRequest(ns.Name, machine.Name))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the ServerClaim image was updated")
		updatedServerClaim := &metalv1alpha1.ServerClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), updatedServerClaim)).To(Succeed())
		Expect(updatedServerClaim.Spec.Image).To(Equal(newImage))
	})

	It("should not update ServerClaim when image already matches", func(ctx SpecContext) {
		const image = "my-image"

		By("creating a MachineClass")
		machineClass := newMachineClassWithImage(ns.Name, "matching-machine-class", image)
		Expect(k8sClient.Create(ctx, machineClass)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineClass)

		By("creating a MachineSet")
		machineSet := newMachineSet(ns.Name, "matching-machine-set", machineClass.Name)
		Expect(k8sClient.Create(ctx, machineSet)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineSet)

		By("creating a Machine with update-result=successful")
		machine := newMachineWithUpdateLabel(ns.Name, "machine-matching", machineSet.Name, machineSet.UID)
		Expect(k8sClient.Create(ctx, machine)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machine)

		By("creating a ServerClaim with the same image")
		serverClaim := newServerClaim(ns.Name, machine.Name, image)
		Expect(k8sClient.Create(ctx, serverClaim)).To(Succeed())
		DeferCleanup(k8sClient.Delete, serverClaim)

		By("running the reconciler")
		metalClientProvider := &mcmclient.Provider{}
		metalClientProvider.SetClient(k8sClient)
		reconciler := NewMachineReconciler(k8sClient, metalClientProvider, ns.Name)
		_, err := reconciler.Reconcile(ctx, reconcileRequest(ns.Name, machine.Name))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the ServerClaim image is unchanged")
		updatedServerClaim := &metalv1alpha1.ServerClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), updatedServerClaim)).To(Succeed())
		Expect(updatedServerClaim.Spec.Image).To(Equal(image))
	})

	It("should skip Machine without owning MachineSet", func(ctx SpecContext) {
		By("creating a Machine with update-result=successful but no owner")
		machine := &machinev1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "machine-no-owner",
				Namespace: ns.Name,
				Labels: map[string]string{
					machinev1alpha1.LabelKeyNodeUpdateResult: machinev1alpha1.LabelValueNodeUpdateSuccessful,
				},
			},
		}
		Expect(k8sClient.Create(ctx, machine)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machine)

		By("creating a ServerClaim with some image")
		serverClaim := newServerClaim(ns.Name, machine.Name, "some-image")
		Expect(k8sClient.Create(ctx, serverClaim)).To(Succeed())
		DeferCleanup(k8sClient.Delete, serverClaim)

		By("running the reconciler")
		metalClientProvider := &mcmclient.Provider{}
		metalClientProvider.SetClient(k8sClient)
		reconciler := NewMachineReconciler(k8sClient, metalClientProvider, ns.Name)
		_, err := reconciler.Reconcile(ctx, reconcileRequest(ns.Name, machine.Name))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the ServerClaim image is unchanged")
		updatedServerClaim := &metalv1alpha1.ServerClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), updatedServerClaim)).To(Succeed())
		Expect(updatedServerClaim.Spec.Image).To(Equal("some-image"))
	})

	It("should skip Machine without update-result label", func(ctx SpecContext) {
		By("creating a MachineClass")
		machineClass := newMachineClassWithImage(ns.Name, "no-label-machine-class", "some-image")
		Expect(k8sClient.Create(ctx, machineClass)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineClass)

		By("creating a MachineSet")
		machineSet := newMachineSet(ns.Name, "no-label-machine-set", machineClass.Name)
		Expect(k8sClient.Create(ctx, machineSet)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineSet)

		By("creating a Machine WITHOUT update-result label")
		machine := &machinev1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "machine-no-label",
				Namespace: ns.Name,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "machine.sapcloud.io/v1alpha1",
						Kind:       "MachineSet",
						Name:       machineSet.Name,
						UID:        machineSet.UID,
						Controller: ptr.To(true),
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, machine)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machine)

		By("creating a ServerClaim with old-image")
		serverClaim := newServerClaim(ns.Name, machine.Name, "old-image")
		Expect(k8sClient.Create(ctx, serverClaim)).To(Succeed())
		DeferCleanup(k8sClient.Delete, serverClaim)

		By("running the reconciler")
		metalClientProvider := &mcmclient.Provider{}
		metalClientProvider.SetClient(k8sClient)
		reconciler := NewMachineReconciler(k8sClient, metalClientProvider, ns.Name)
		_, err := reconciler.Reconcile(ctx, reconcileRequest(ns.Name, machine.Name))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the ServerClaim image is unchanged (no update since no label)")
		updatedServerClaim := &metalv1alpha1.ServerClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), updatedServerClaim)).To(Succeed())
		Expect(updatedServerClaim.Spec.Image).To(Equal("old-image"))
	})

	It("should skip gracefully when ServerClaim does not exist", func(ctx SpecContext) {
		By("creating a MachineClass with new image")
		machineClass := newMachineClassWithImage(ns.Name, "no-claim-machine-class", "new-image")
		Expect(k8sClient.Create(ctx, machineClass)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineClass)

		By("creating a MachineSet")
		machineSet := newMachineSet(ns.Name, "no-claim-machine-set", machineClass.Name)
		Expect(k8sClient.Create(ctx, machineSet)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machineSet)

		By("creating a Machine with update-result=successful but no ServerClaim")
		machine := newMachineWithUpdateLabel(ns.Name, "machine-no-claim", machineSet.Name, machineSet.UID)
		Expect(k8sClient.Create(ctx, machine)).To(Succeed())
		DeferCleanup(k8sClient.Delete, machine)

		By("running the reconciler - should not error when ServerClaim is missing")
		metalClientProvider := &mcmclient.Provider{}
		metalClientProvider.SetClient(k8sClient)
		reconciler := NewMachineReconciler(k8sClient, metalClientProvider, ns.Name)
		_, err := reconciler.Reconcile(ctx, reconcileRequest(ns.Name, machine.Name))
		Expect(err).NotTo(HaveOccurred())
	})
})
