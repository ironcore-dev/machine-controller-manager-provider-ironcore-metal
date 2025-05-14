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
	"k8s.io/klog/v2"
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

	return &driver.UpdateMachineResponse{}, nil
}

func isEmptyUpdateRequest(req *driver.UpdateMachineRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}
