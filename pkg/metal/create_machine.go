// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	LabelKeyServerClaimName      = "metal.ironcore.dev/server-claim-name"
	LabelKeyServerClaimNamespace = "metal.ironcore.dev/server-claim-namespace"
)

// CreateMachine handles a machine creation request
func (d *metalDriver) CreateMachine(ctx context.Context, req *driver.CreateMachineRequest) (*driver.CreateMachineResponse, error) {
	if isEmptyCreateRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "received empty request")
	}
	if req.MachineClass.Provider != apiv1alpha1.ProviderName {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("requested provider '%s' is not supported by the driver '%s'", req.MachineClass.Provider, apiv1alpha1.ProviderName))
	}

	klog.V(3).Infof("Machine creation request has been received for %s", req.Machine.Name)
	defer klog.V(3).Infof("Machine creation request has been processed for %s", req.Machine.Name)

	providerSpec, err := validateProviderSpecAndSecret(req.MachineClass, req.Secret)
	if err != nil {
		return nil, err
	}

	addressClaims, addressesMetaData, err := d.getOrCreateIPAddressClaims(ctx, req.Machine, providerSpec)
	if err != nil {
		return nil, err
	}

	ignitionSecret, err := d.generateIgnitionSecret(ctx, req.Machine, req.Secret, providerSpec, addressesMetaData)
	if err != nil {
		return nil, err
	}

	serverClaim, err := d.applyServerClaim(ctx, req.Machine, providerSpec, ignitionSecret)
	if err != nil {
		return nil, err
	}

	if err := d.setServerClaimOwnership(ctx, serverClaim, addressClaims); err != nil {
		return nil, err
	}

	return &driver.CreateMachineResponse{
		ProviderID: getProviderIDForServerClaim(serverClaim),
		NodeName:   serverClaim.Name,
	}, nil
}

// isEmptyCreateRequest checks if any of the fields in CreateMachineRequest is empty
func isEmptyCreateRequest(req *driver.CreateMachineRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}

// setServerClaimOwnership sets the owner reference of the IPAddressClaims to the ServerClaim
func (d *metalDriver) setServerClaimOwnership(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim, IPAddressClaims []*capiv1beta1.IPAddressClaim) error {
	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client

	// wait for the server claim to be visible in a cache
	err := wait.PollUntilContextTimeout(
		ctx,
		50*time.Millisecond,
		340*time.Millisecond,
		true,
		func(ctx context.Context) (bool, error) {
			if err := metalClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), serverClaim); err != nil {
				return false, err
			}
			return true, nil
		})
	if err != nil {
		return err
	}

	for _, IPAddressClaim := range IPAddressClaims {
		IPAddressClaimCopy := IPAddressClaim.DeepCopy()
		if err := controllerutil.SetOwnerReference(serverClaim, IPAddressClaim, metalClient.Scheme()); err != nil {
			return fmt.Errorf("failed to set OwnerReference: %w", err)
		}
		if err := metalClient.Patch(ctx, IPAddressClaim, client.MergeFrom(IPAddressClaimCopy)); err != nil {
			return fmt.Errorf("failed to patch IPAddressClaim: %w", err)
		}
	}

	return nil
}

// validateProviderSpecAndSecret Validates providerSpec and provider secret
func validateProviderSpecAndSecret(class *machinev1alpha1.MachineClass, secret *corev1.Secret) (*apiv1alpha1.ProviderSpec, error) {
	if class == nil {
		return nil, status.Error(codes.Internal, "MachineClass in ProviderSpec is not set")
	}

	var providerSpec *apiv1alpha1.ProviderSpec
	if err := json.Unmarshal(class.ProviderSpec.Raw, &providerSpec); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	validationErr := validation.ValidateProviderSpecAndSecret(providerSpec, secret, field.NewPath("providerSpec"))
	if validationErr.ToAggregate() != nil && len(validationErr.ToAggregate().Errors()) > 0 {
		err := fmt.Errorf("failed to validate provider spec and secret: %v", validationErr.ToAggregate().Errors())
		return nil, status.Error(codes.Internal, err.Error())
	}

	return providerSpec, nil
}
