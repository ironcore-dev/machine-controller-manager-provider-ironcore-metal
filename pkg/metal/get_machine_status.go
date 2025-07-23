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
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetMachineStatus handles a machine get status request
func (d *metalDriver) GetMachineStatus(ctx context.Context, req *driver.GetMachineStatusRequest) (*driver.GetMachineStatusResponse, error) {
	if isEmptyMachineStatusRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "received empty GetMachineStatusRequest")
	}

	if req.MachineClass.Provider != apiv1alpha1.ProviderName {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("requested provider %q is not supported by the driver %q", req.MachineClass.Provider, apiv1alpha1.ProviderName))
	}

	klog.V(3).Infof("Machine status request has been received for %q", req.Machine.Name)
	defer klog.V(3).Infof("Machine status request has been processed for %q", req.Machine.Name)

	providerSpec, err := GetProviderSpec(req.MachineClass, req.Secret)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get provider spec: %v", err))
	}

	serverClaim := &metalv1alpha1.ServerClaim{}

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Get(ctx, client.ObjectKey{Namespace: d.metalNamespace, Name: req.Machine.Name}, serverClaim)
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if len(serverClaim.Annotations) > 0 && serverClaim.Annotations[validation.AnnotationKeyMCMMachineRecreate] == "true" {
		klog.V(3).Infof("Machine creation flow will be retriggered, Server still not bound: %q", req.Machine.Name)
		// MCM provider retry with codes.NotFound which triggers machine creation flow
		return nil, status.Error(codes.NotFound, fmt.Sprintf("server claim %q is marked for recreation", req.Machine.Name))
	}

	if err := d.validateIPAddressClaims(ctx, req, serverClaim, providerSpec); err != nil {
		klog.V(3).Infof("Machine creation flow will be retriggered, IPAddressClaims validation was unsuccessful: %q", req.Machine.Name)
		// MCM provider retry with codes.NotFound which triggers machine creation flow
		return nil, status.Error(codes.NotFound, fmt.Sprintf("unsuccessful IPAddressClaims validation, will recreate: %v", err))
	}

	if serverClaim.Spec.Power != metalv1alpha1.PowerOn {
		klog.V(3).Infof("Machine initialization flow will be retriggered, Server still not powered on %q", req.Machine.Name)
		// MCM provider retry with codes.Uninitialized which triggers machine initialization flow
		return nil, status.Error(codes.Uninitialized, fmt.Sprintf("server claim %q is still not powered on, will reinitialize", req.Machine.Name))
	}

	nodeName, err := getNodeName(ctx, d.nodeNamePolicy, serverClaim, d.metalNamespace, d.clientProvider)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get node name: %v", err))
	}

	return &driver.GetMachineStatusResponse{
		ProviderID: getProviderIDForServerClaim(serverClaim),
		NodeName:   nodeName,
	}, nil
}

func isEmptyMachineStatusRequest(req *driver.GetMachineStatusRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}

func (d *metalDriver) validateIPAddressClaims(ctx context.Context, req *driver.GetMachineStatusRequest, serverClaim *metalv1alpha1.ServerClaim, providerSpec *apiv1alpha1.ProviderSpec) error {
	klog.V(3).Info("Validating IPAddressClaims", "name", req.Machine.Name, "namespace", d.metalNamespace)

	for _, ipamConfig := range providerSpec.IPAMConfig {
		if ipamConfig.IPAMRef == nil {
			return fmt.Errorf("IPAMRef of an IPAMConfig %q is not set", ipamConfig.MetadataKey)
		}

		ipClaim := &capiv1beta1.IPAddressClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      getIPAddressClaimName(req.Machine.Name, ipamConfig.MetadataKey),
				Namespace: d.metalNamespace,
			},
		}

		if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
			return metalClient.Get(ctx, client.ObjectKeyFromObject(ipClaim), ipClaim)
		}); err != nil {
			return fmt.Errorf("failed to get IPAddressClaim %q: %v", ipClaim.Name, err)
		}

		validationErr := validation.ValidateIPAddressClaim(ipClaim, serverClaim, req.Machine.Name, d.metalNamespace)
		if validationErr.ToAggregate() != nil && len(validationErr.ToAggregate().Errors()) > 0 {
			return fmt.Errorf("failed to validate IPAddressClaim %s/%s: %v", ipClaim.Namespace, ipClaim.Name, validationErr.ToAggregate().Errors())
		}

		if ipClaim.Status.AddressRef.Name == "" {
			return fmt.Errorf("IPAddressClaim %s/%s still not bound", ipClaim.Namespace, ipClaim.Name)
		}
	}

	klog.V(3).Info("All IPAddressClaims are valid and bound", "name", req.Machine.Name, "namespace", d.metalNamespace)
	return nil
}
