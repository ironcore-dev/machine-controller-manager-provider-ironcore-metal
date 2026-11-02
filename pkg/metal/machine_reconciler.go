// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultMachineReconcilerSyncPeriod = time.Minute

// MachineReconciler keeps ServerClaim images in sync with the MachineClass image after in-place node updates.
//
// MCM transfers Machine ownership to a new MachineSet after a successful in-place update, but does not call
// any provider driver method to notify the provider of the new MachineClass image. This reconciler detects
// transferred machines (via the LabelKeyNodeUpdateResult: successful label) and patches the corresponding
// ServerClaim to match the new MachineClass image.
type MachineReconciler struct {
	controlClient       client.Client
	metalClientProvider *mcmclient.Provider
	metalNamespace      string
	namespace           string
	syncPeriod          time.Duration
}

// NewMachineReconciler creates a MachineReconciler. If syncPeriod is zero, defaultMachineReconcilerSyncPeriod is used.
func NewMachineReconciler(
	controlClient client.Client,
	metalClientProvider *mcmclient.Provider,
	metalNamespace, namespace string,
	syncPeriod time.Duration,
) *MachineReconciler {
	if syncPeriod == 0 {
		syncPeriod = defaultMachineReconcilerSyncPeriod
	}
	return &MachineReconciler{
		controlClient:       controlClient,
		metalClientProvider: metalClientProvider,
		metalNamespace:      metalNamespace,
		namespace:           namespace,
		syncPeriod:          syncPeriod,
	}
}

// Start runs the sync loop until ctx is cancelled.
func (r *MachineReconciler) Start(ctx context.Context) {
	klog.V(3).InfoS("Starting MachineReconciler", "namespace", r.namespace, "syncPeriod", r.syncPeriod)
	ticker := time.NewTicker(r.syncPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := r.syncAll(ctx); err != nil {
				klog.Errorf("MachineReconciler sync failed: %v", err)
			}
		case <-ctx.Done():
			klog.V(3).InfoS("MachineReconciler stopped")
			return
		}
	}
}

func (r *MachineReconciler) syncAll(ctx context.Context) error {
	machineList := &machinev1alpha1.MachineList{}
	if err := r.controlClient.List(ctx, machineList,
		client.InNamespace(r.namespace),
		client.MatchingLabels{
			machinev1alpha1.LabelKeyNodeUpdateResult: machinev1alpha1.LabelValueNodeUpdateSuccessful,
		},
	); err != nil {
		return fmt.Errorf("failed to list machines with %s=%s: %w",
			machinev1alpha1.LabelKeyNodeUpdateResult, machinev1alpha1.LabelValueNodeUpdateSuccessful, err)
	}

	klog.V(4).InfoS("MachineReconciler found machines pending image sync", "count", len(machineList.Items))
	for i := range machineList.Items {
		if err := r.syncMachine(ctx, &machineList.Items[i]); err != nil {
			klog.Errorf("failed to sync machine %s/%s: %v", machineList.Items[i].Namespace, machineList.Items[i].Name, err)
		}
	}
	return nil
}

func (r *MachineReconciler) syncMachine(ctx context.Context, machine *machinev1alpha1.Machine) error {
	machineSetName := controllerOwnerName(machine)
	if machineSetName == "" {
		klog.V(4).InfoS("Machine has no controlling MachineSet, skipping", "machine", machine.Name)
		return nil
	}

	machineSet := &machinev1alpha1.MachineSet{}
	if err := r.controlClient.Get(ctx, client.ObjectKey{Namespace: r.namespace, Name: machineSetName}, machineSet); err != nil {
		return fmt.Errorf("failed to get MachineSet %s: %w", machineSetName, err)
	}

	machineClassName := machineSet.Spec.MachineClass.Name
	if machineClassName == "" {
		return fmt.Errorf("MachineSet %s has empty MachineClass name", machineSetName)
	}

	machineClass := &machinev1alpha1.MachineClass{}
	if err := r.controlClient.Get(ctx, client.ObjectKey{Namespace: r.namespace, Name: machineClassName}, machineClass); err != nil {
		return fmt.Errorf("failed to get MachineClass %s: %w", machineClassName, err)
	}

	var providerSpec apiv1alpha1.ProviderSpec
	if err := json.Unmarshal(machineClass.ProviderSpec.Raw, &providerSpec); err != nil {
		return fmt.Errorf("failed to parse ProviderSpec for MachineClass %s: %w", machineClassName, err)
	}
	if providerSpec.Image == "" {
		return fmt.Errorf("MachineClass %s has empty image in ProviderSpec", machineClassName)
	}

	serverClaim := &metalv1alpha1.ServerClaim{}
	if err := r.metalClientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Get(ctx, client.ObjectKey{Namespace: r.metalNamespace, Name: machine.Name}, serverClaim)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).InfoS("ServerClaim not found for machine, skipping", "machine", machine.Name)
			return nil
		}
		return fmt.Errorf("failed to get ServerClaim for machine %s: %w", machine.Name, err)
	}

	if serverClaim.Spec.Image == providerSpec.Image {
		return nil
	}

	klog.V(3).InfoS("Updating ServerClaim image after in-place update",
		"machine", machine.Name,
		"oldImage", serverClaim.Spec.Image,
		"newImage", providerSpec.Image)

	if err := patchServerClaimImage(ctx, r.metalClientProvider, serverClaim, providerSpec.Image); err != nil {
		return fmt.Errorf("failed to update ServerClaim image for machine %s: %w", machine.Name, err)
	}

	klog.V(3).InfoS("Successfully updated ServerClaim image", "machine", machine.Name, "image", providerSpec.Image)
	return nil
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
