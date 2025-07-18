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
	"k8s.io/klog/v2"
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

	klog.V(3).Infof("machine status request has been received for %q", req.Machine.Name)
	defer klog.V(3).Infof("machine status request has been processed for %q", req.Machine.Name)

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
		klog.V(3).Infof("machine creation flow will be retriggered, Server still not bound %q", req.Machine.Name)
		// MCM provider retry with codes.NotFound which triggers machine create flow
		return nil, status.Error(codes.NotFound, fmt.Sprintf("server claim %q is marked for recreation", req.Machine.Name))
	}

	if serverClaim.Spec.Power != metalv1alpha1.PowerOn {
		klog.V(3).Infof("machine initialization flow will be retriggered, Server still not powered on %q", req.Machine.Name)
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
