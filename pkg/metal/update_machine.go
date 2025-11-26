// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"fmt"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (d *metalDriver) UpdateMachine(ctx context.Context, req *driver.UpdateMachineRequest) (*driver.UpdateMachineResponse, error) {
	if isEmptyUpdateRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "received empty request")
	}

	if req.MachineClass.Provider != apiv1alpha1.ProviderName {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("requested provider '%s' is not supported by the driver '%s'", req.MachineClass.Provider, apiv1alpha1.ProviderName))
	}

	klog.V(3).Infof("Machine update request has been received for %q", req.Machine.Name)
	defer klog.V(3).Infof("Machine update request has been processed for %q", req.Machine.Name)

	providerSpec, err := getProviderSpecForMachineClass(req.MachineClass, req.Secret)
	if err != nil {
		return nil, err
	}
	klog.V(3).Infof("Generated provider spec for machine %q", req.Machine.Name)

	serverClaim, err := d.getServerClaimForMachine(ctx, req.Machine)
	if err != nil {
		return nil, err
	}
	klog.V(3).Infof("Got server claim for machine %q", req.Machine.Name)

	nodeName, err := getNodeName(ctx, d.nodeNamePolicy, serverClaim, d.metalNamespace, d.clientProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to get node name: %w", err)
	}
	klog.V(3).Infof("Got node name %s for machine %q", nodeName, req.Machine.Name)

	addressesMetaData, err := d.collectIPAddressClaimsMetadata(ctx, req.Machine, providerSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to collect addresses: %w", err)
	}
	klog.V(3).Infof("Got addresses meta data for machine %q", req.Machine.Name)

	serverMetadata, err := d.extractServerMetadataFromClaim(ctx, serverClaim)
	if err != nil {
		return nil, fmt.Errorf("error extracting server metadata from ServerClaim %q: %w", client.ObjectKeyFromObject(serverClaim), err)
	}
	klog.V(3).Infof("Got server metadata for machine %q", req.Machine.Name)

	ignitionSecret, err := d.generateIgnitionSecret(ctx, req.Machine, req.MachineClass, req.Secret, nodeName, providerSpec, addressesMetaData, serverMetadata)
	if err != nil {
		return nil, err
	}
	klog.V(3).Infof("Generated ignition secret for machine %q", req.Machine.Name)

	if err := d.applyIgnitionAndRestartServer(ctx, ignitionSecret, serverClaim, providerSpec); err != nil {
		return nil, err
	}
	klog.V(3).Infof("Applying server claim for machine %q", req.Machine.Name)

	return &driver.UpdateMachineResponse{}, nil
}

func (d *metalDriver) applyIgnitionAndRestartServer(ctx context.Context, secret *corev1.Secret, claim *metalv1alpha1.ServerClaim, providerSpec *apiv1alpha1.ProviderSpec) error {
	claimBase := claim.DeepCopy()
	claim.Annotations = map[string]string{
		metalv1alpha1.OperationAnnotation: metalv1alpha1.GracefulRestartServerPower,
	}
	claim.Spec.Image = providerSpec.Image

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Patch(ctx, claim, client.MergeFrom(claimBase))
	}); err != nil {
		return fmt.Errorf("failed to apply ServerClaim: %w", err)
	}

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Patch(ctx, secret, client.Apply, fieldOwner, client.ForceOwnership)
	}); err != nil {
		return fmt.Errorf("failed to apply Ignition secret: %w", err)
	}

	return nil
}

func isEmptyUpdateRequest(req *driver.UpdateMachineRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}
