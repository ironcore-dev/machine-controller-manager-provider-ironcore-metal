// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/imdario/mergo"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/ignition"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func getProviderSpecForMachineClass(machineClass *machinev1alpha1.MachineClass, secret *corev1.Secret) (*apiv1alpha1.ProviderSpec, error) {
	if machineClass == nil {
		return nil, errors.New("MachineClass is not set in request")
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

// IsServerBound checks if the server is already bound
func (d *metalDriver) IsServerBound(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim) (bool, error) {
	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), serverClaim)
	}); err != nil {
		return false, fmt.Errorf("failed to get ServerClaim %q: %v", serverClaim.Name, err)
	}

	return serverClaim.Spec.ServerRef != nil, nil
}

func (d *metalDriver) nodeExistsByName(ctx context.Context, nodeName string) bool {
	nodeFound := false

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		nodeList := &corev1.NodeList{}
		err := metalClient.List(ctx, nodeList)
		if err != nil {
			return err
		}
		for _, node := range nodeList.Items {
			if node.Name == nodeName {
				nodeFound = true
				break
			}
		}
		return nil
	}); err != nil {
		klog.V(3).Info("Failed to list nodes", "error", err)
	}

	return nodeFound
}

func (d *metalDriver) getServerClaimForMachine(ctx context.Context, machine *machinev1alpha1.Machine) (*metalv1alpha1.ServerClaim, error) {
	if machine == nil {
		return nil, fmt.Errorf("requested machine is nil")
	}

	klog.V(3).Info("Getting ServerClaim for Machine", "Machine", client.ObjectKeyFromObject(machine))
	serverClaim := &metalv1alpha1.ServerClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      machine.Name,
			Namespace: d.metalNamespace,
		},
	}

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), serverClaim)
	}); err != nil {
		return nil, fmt.Errorf("failed to get ServerClaim %q: %w", client.ObjectKeyFromObject(serverClaim), err)
	}

	return serverClaim, nil
}

// generateIgnition creates an ignition file for the machine and stores it in a secret
func (d *metalDriver) generateIgnitionSecret(ctx context.Context,
	machine *machinev1alpha1.Machine,
	machineClass *machinev1alpha1.MachineClass,
	machineSecret *corev1.Secret,
	nodeName string,
	providerSpec *apiv1alpha1.ProviderSpec,
	addressesMetaData map[string]any,
	serverMetadata *ServerMetadata) (*corev1.Secret, error) {
	if machine == nil {
		return nil, fmt.Errorf("machine is nil")
	}
	if machineClass == nil {
		return nil, fmt.Errorf("machineClass is nil")
	}
	if machineSecret == nil {
		return nil, fmt.Errorf("machineSecret is nil")
	}

	klog.V(3).Info("Generating Ignition secret for Machine", "Machine", client.ObjectKeyFromObject(machine))
	userData, ok := machineSecret.Data["userData"]
	if !ok {
		return nil, fmt.Errorf("failed to find user-data in Secret %q", client.ObjectKeyFromObject(machineSecret))
	}

	if providerSpec.Metadata == nil {
		providerSpec.Metadata = make(map[string]any)
	}

	if serverMetadata != nil {
		metadata := map[string]any{}
		if serverMetadata.LoopbackAddress != nil {
			metadata["loopbackAddress"] = serverMetadata.LoopbackAddress.String()
		}
		if err := mergo.Merge(&providerSpec.Metadata, metadata, mergo.WithOverride); err != nil {
			return nil, fmt.Errorf("failed to merge server metadata into provider metadata: %w", err)
		}
	}

	if err := mergo.Merge(&providerSpec.Metadata, addressesMetaData, mergo.WithOverride); err != nil {
		return nil, fmt.Errorf("failed to merge addresses metadata into provider metadata: %w", err)
	}

	config := &ignition.Config{
		Hostname:         nodeName,
		UserData:         string(userData),
		MetaData:         providerSpec.Metadata,
		Ignition:         providerSpec.Ignition,
		DnsServers:       providerSpec.DnsServers,
		IgnitionOverride: providerSpec.IgnitionOverride,
	}

	ignitionContent, err := ignition.Render(config)
	if err != nil {
		return nil, fmt.Errorf("failed to render ignition for Machine %q: %w", client.ObjectKeyFromObject(machine), err)
	}

	ignitionData := map[string][]byte{}
	ignitionData["ignition"] = []byte(ignitionContent)
	ignitionSecret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.getIgnitionNameForMachine(ctx, machine.Name),
			Namespace: d.metalNamespace,
		},
		Data: ignitionData,
	}

	return ignitionSecret, nil
}
