// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultIgnitionKey     = "ignition"
	ShootNameLabelKey      = "shoot-name"
	ShootNamespaceLabelKey = "shoot-namespace"
)

var (
	fieldOwner = client.FieldOwner("mcm.ironcore.dev/field-owner")
)

type metalDriver struct {
	Schema         *runtime.Scheme
	clientProvider *mcmclient.Provider
	metalNamespace string
	nodeNamePolicy cmd.NodeNamePolicy
}

func (d *metalDriver) GetVolumeIDs(_ context.Context, _ *driver.GetVolumeIDsRequest) (*driver.GetVolumeIDsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Metal Provider does not yet implement GetVolumeIDs")
}

// NewDriver returns a new Gardener metal driver object
func NewDriver(clientProvider *mcmclient.Provider, namespace string, nodeNamePolicy cmd.NodeNamePolicy) driver.Driver {
	return &metalDriver{
		clientProvider: clientProvider,
		metalNamespace: namespace,
		nodeNamePolicy: nodeNamePolicy,
	}
}

func (d *metalDriver) GenerateMachineClassForMigration(_ context.Context, _ *driver.GenerateMachineClassForMigrationRequest) (*driver.GenerateMachineClassForMigrationResponse, error) {
	return &driver.GenerateMachineClassForMigrationResponse{}, nil
}

func (d *metalDriver) getIgnitionNameForMachine(ctx context.Context, machineName string) string {
	//for backward compatibility checking if the ignition secret was already present with the old naming convention
	ignitionSecretName := fmt.Sprintf("%s-%s", machineName, "ignition")
	if err := d.clientProvider.SyncClient(func(k8s client.Client) error {
		return k8s.Get(ctx, client.ObjectKey{Name: ignitionSecretName, Namespace: d.metalNamespace}, &corev1.Secret{})
	}); apierrors.IsNotFound(err) {
		return machineName
	}
	return ignitionSecretName
}

func getProviderIDForServerClaim(serverClaim *metalv1alpha1.ServerClaim) string {
	return fmt.Sprintf("%s://%s/%s", apiv1alpha1.ProviderName, serverClaim.Namespace, serverClaim.Name)
}

func getNodeName(ctx context.Context, policy cmd.NodeNamePolicy, serverClaim *metalv1alpha1.ServerClaim, metalNamespace string, clientProvider *mcmclient.Provider) (string, error) {
	switch policy {
	case cmd.NodeNamePolicyServerClaimName:
		return serverClaim.Name, nil
	case cmd.NodeNamePolicyServerName:
		if serverClaim.Spec.ServerRef == nil {
			return "", errors.New("server claim does not have a server ref")
		}
		return serverClaim.Spec.ServerRef.Name, nil
	case cmd.NodeNamePolicyBMCName:
		if serverClaim.Spec.ServerRef == nil {
			return "", errors.New("server claim does not have a server ref")
		}
		var server metalv1alpha1.Server
		if err := clientProvider.SyncClient(func(metalClient client.Client) error {
			return metalClient.Get(ctx, client.ObjectKey{Namespace: metalNamespace, Name: serverClaim.Spec.ServerRef.Name}, &server)
		}); err != nil {
			return "", fmt.Errorf("failed to get server %q: %v", serverClaim.Spec.ServerRef.Name, err)
		}
		if server.Spec.BMCRef == nil {
			return "", fmt.Errorf("server %q does not have a BMC configured", serverClaim.Spec.ServerRef.Name)
		}
		return server.Spec.BMCRef.Name, nil
	}
	return "", fmt.Errorf("unknown node name policy: %s", policy)
}

func getIPAddressClaimName(machineName, metadataKey string) string {
	ipAddrClaimName := fmt.Sprintf("%s-%s", machineName, metadataKey)
	if len(ipAddrClaimName) > utilvalidation.DNS1123SubdomainMaxLength {
		klog.Info("IPAddressClaim name is too long, it will be shortened which can cause name collisions", "name", ipAddrClaimName)
		ipAddrClaimName = ipAddrClaimName[:utilvalidation.DNS1123SubdomainMaxLength]
	}
	return ipAddrClaimName
}

func GetProviderSpec(machineClass *machinev1alpha1.MachineClass, secret *corev1.Secret) (*apiv1alpha1.ProviderSpec, error) {
	if machineClass == nil {
		return nil, status.Error(codes.Internal, "MachineClass is not set in request")
	}

	var providerSpec *apiv1alpha1.ProviderSpec
	if err := json.Unmarshal(machineClass.ProviderSpec.Raw, &providerSpec); err != nil {
		return nil, err
	}

	validationErr := validation.ValidateProviderSpecAndSecret(providerSpec, secret, field.NewPath("providerSpec"))
	if validationErr.ToAggregate() != nil && len(validationErr.ToAggregate().Errors()) > 0 {
		return nil, fmt.Errorf("failed to validate provider spec and secret: %v", validationErr.ToAggregate().Errors())
	}

	return providerSpec, nil
}
