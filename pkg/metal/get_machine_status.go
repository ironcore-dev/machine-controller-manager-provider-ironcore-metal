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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetMachineStatus handles a machine get status request
func (d *metalDriver) GetMachineStatus(ctx context.Context, req *driver.GetMachineStatusRequest) (*driver.GetMachineStatusResponse, error) {
	if isEmptyMachineStatusRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "received empty request")
	}
	if req.MachineClass.Provider != apiv1alpha1.ProviderName {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("requested provider '%s' is not supported by the driver '%s'", req.MachineClass.Provider, apiv1alpha1.ProviderName))
	}

	klog.V(3).Infof("Machine status request has been received for %q", req.Machine.Name)
	defer klog.V(3).Infof("Machine status request has been processed for %q", req.Machine.Name)

	serverClaim := &metalv1alpha1.ServerClaim{}

	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	if err := d.clientProvider.Client.Get(ctx, client.ObjectKey{Namespace: d.metalNamespace, Name: req.Machine.Name}, serverClaim); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &driver.GetMachineStatusResponse{
		ProviderID: getProviderIDForServerClaim(serverClaim),
		NodeName:   serverClaim.Name,
	}, nil
}

func isEmptyMachineStatusRequest(req *driver.GetMachineStatusRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}
