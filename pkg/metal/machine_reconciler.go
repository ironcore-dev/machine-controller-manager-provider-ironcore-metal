// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"fmt"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// MachineReconciler keeps ServerClaim images in sync with the MachineClass image after in-place node updates.
//
// MCM transfers Machine ownership to a new MachineSet after a successful in-place update, but does not call
// any provider driver method to notify the provider of the new MachineClass image. This reconciler watches
// Machine objects for the LabelKeyNodeUpdateResult: successful label that MCM sets on transfer, resolves
// the new image via the owner MachineSet → MachineClass → ProviderSpec chain, and patches the ServerClaim.
type MachineReconciler struct {
	client              client.Client
	metalClientProvider *mcmclient.Provider
	metalNamespace      string
}

// NewMachineReconciler creates a MachineReconciler.
func NewMachineReconciler(controlClient client.Client, metalClientProvider *mcmclient.Provider, metalNamespace string) *MachineReconciler {
	return &MachineReconciler{
		client:              controlClient,
		metalClientProvider: metalClientProvider,
		metalNamespace:      metalNamespace,
	}
}

// Reconcile is called whenever a Machine with the update-result=successful label is created or updated.
func (r *MachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	machine := &machinev1alpha1.Machine{}
	if err := r.client.Get(ctx, req.NamespacedName, machine); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if machine.Labels[machinev1alpha1.LabelKeyNodeUpdateResult] != machinev1alpha1.LabelValueNodeUpdateSuccessful {
		return ctrl.Result{}, nil
	}

	machineSetName := controllerOwnerName(machine)
	if machineSetName == "" {
		klog.V(4).InfoS("Machine has no controlling MachineSet, skipping", "machine", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	machineSet := &machinev1alpha1.MachineSet{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: machineSetName}, machineSet); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get MachineSet %s: %w", machineSetName, err)
	}

	machineClassName := machineSet.Spec.MachineClass.Name
	if machineClassName == "" {
		return ctrl.Result{}, fmt.Errorf("MachineSet %s has empty MachineClass name", machineSetName)
	}

	machineClass := &machinev1alpha1.MachineClass{}
	if err := r.client.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: machineClassName}, machineClass); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get MachineClass %s: %w", machineClassName, err)
	}

	var providerSpec apiv1alpha1.ProviderSpec
	if err := json.Unmarshal(machineClass.ProviderSpec.Raw, &providerSpec); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to parse ProviderSpec for MachineClass %s: %w", machineClassName, err)
	}
	if providerSpec.Image == "" {
		return ctrl.Result{}, fmt.Errorf("MachineClass %s has empty image in ProviderSpec", machineClassName)
	}

	serverClaim := &metalv1alpha1.ServerClaim{}
	if err := r.metalClientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Get(ctx, client.ObjectKey{Namespace: r.metalNamespace, Name: machine.Name}, serverClaim)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).InfoS("ServerClaim not found for machine, skipping", "machine", req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get ServerClaim for machine %s: %w", machine.Name, err)
	}

	if serverClaim.Spec.Image == providerSpec.Image {
		return ctrl.Result{}, nil
	}

	klog.V(3).InfoS("Updating ServerClaim image after in-place update",
		"machine", req.NamespacedName,
		"oldImage", serverClaim.Spec.Image,
		"newImage", providerSpec.Image)

	if err := patchServerClaimImage(ctx, r.metalClientProvider, serverClaim, providerSpec.Image); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update ServerClaim image for machine %s: %w", machine.Name, err)
	}

	klog.V(3).InfoS("Successfully updated ServerClaim image", "machine", req.NamespacedName, "image", providerSpec.Image)
	return ctrl.Result{}, nil
}

// SetupWithManager registers the MachineReconciler with the given manager.
// It only reconciles Machine objects that carry the update-result=successful label.
func (r *MachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&machinev1alpha1.Machine{}).
		WithEventFilter(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				return e.Object.GetLabels()[machinev1alpha1.LabelKeyNodeUpdateResult] == machinev1alpha1.LabelValueNodeUpdateSuccessful
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				return e.ObjectNew.GetLabels()[machinev1alpha1.LabelKeyNodeUpdateResult] == machinev1alpha1.LabelValueNodeUpdateSuccessful
			},
			DeleteFunc:  func(event.DeleteEvent) bool { return false },
			GenericFunc: func(event.GenericEvent) bool { return false },
		}).
		Complete(r)
}

// controllerOwnerName returns the name of the controlling MachineSet from the Machine's ownerReferences.
func controllerOwnerName(machine *machinev1alpha1.Machine) string {
	for _, ref := range machine.OwnerReferences {
		if ref.Kind == "MachineSet" && ref.Controller != nil && *ref.Controller {
			return ref.Name
		}
	}
	return ""
}

// patchServerClaimImage patches serverClaim.Spec.Image to newImage.
func patchServerClaimImage(ctx context.Context, metalClientProvider *mcmclient.Provider, serverClaim *metalv1alpha1.ServerClaim, newImage string) error {
	base := serverClaim.DeepCopy()
	serverClaim.Spec.Image = newImage
	return metalClientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Patch(ctx, serverClaim, client.MergeFrom(base))
	})
}
