// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"fmt"
	"maps"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (d *metalDriver) ListMachines(ctx context.Context, req *driver.ListMachinesRequest) (*driver.ListMachinesResponse, error) {
	if isEmptyListMachinesRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "received empty ListMachinesRequest")
	}

	if req.MachineClass.Provider != apiv1alpha1.ProviderName {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("requested provider %q is not supported by the driver %q", req.MachineClass.Provider, apiv1alpha1.ProviderName))
	}

	klog.V(3).Infof("machine list request has been received for %q", req.MachineClass.Name)
	defer klog.V(3).Infof("machine list request has been processed for %q", req.MachineClass.Name)

	providerSpec, err := GetProviderSpec(req.MachineClass, req.Secret)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get provider spec: %v", err))
	}

	serverClaimList := &metalv1alpha1.ServerClaimList{}
	matchingLabels := client.MatchingLabels{}
	maps.Copy(matchingLabels, providerSpec.Labels)

	if err = d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.List(ctx, serverClaimList, client.InNamespace(d.metalNamespace), matchingLabels)
	}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	machineList := make(map[string]string, len(serverClaimList.Items))
	for _, machine := range serverClaimList.Items {
		machineID := getProviderIDForServerClaim(&machine)
		machineList[machineID] = machine.Name
	}

	return &driver.ListMachinesResponse{MachineList: machineList}, nil
}

func isEmptyListMachinesRequest(req *driver.ListMachinesRequest) bool {
	return req == nil || req.MachineClass == nil || req.Secret == nil
}
