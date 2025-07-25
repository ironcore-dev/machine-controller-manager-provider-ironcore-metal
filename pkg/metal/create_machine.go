// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/imdario/mergo"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/ignition"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
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

	addressClaims, addressesMetaData, err := d.getOrCreateIPAddressClaims(ctx, req, providerSpec)
	if err != nil {
		return nil, err
	}

	ignitionSecret, err := d.generateIgnition(ctx, req, req.Machine.Name, providerSpec, addressesMetaData, nil)
	if err != nil {
		return nil, err
	}

	serverClaim := d.generateServerClaim(req, providerSpec, ignitionSecret)

	serverClaim, err = d.createServerClaim(ctx, serverClaim, ignitionSecret)
	if err != nil {
		return nil, err
	}

	if err := d.setServerClaimOwnershipToIPAddressClaim(ctx, serverClaim, addressClaims); err != nil {
		return nil, err
	}

	err = d.waitForServerClaim(ctx, serverClaim)
	if err != nil {
		return nil, err
	}

	d.clientProvider.Lock()
	nodeName, err := GetNodeName(ctx, d.nodeNamePolicy, serverClaim, d.metalNamespace, d.clientProvider.Client)
	d.clientProvider.Unlock()
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get node name: %v", err))
	}

	if err := d.updateIgnitionAndPowerOnServer(ctx, req, serverClaim, providerSpec, addressesMetaData, nodeName); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &driver.CreateMachineResponse{
		ProviderID: getProviderIDForServerClaim(serverClaim),
		NodeName:   nodeName,
	}, nil
}

func (d *metalDriver) generateServerClaim(req *driver.CreateMachineRequest, spec *apiv1alpha1.ProviderSpec, secret *corev1.Secret) *metalv1alpha1.ServerClaim {
	return &metalv1alpha1.ServerClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: metalv1alpha1.GroupVersion.String(),
			Kind:       "ServerClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Machine.Name,
			Namespace: d.metalNamespace,
			Labels:    spec.Labels,
		},
		Spec: metalv1alpha1.ServerClaimSpec{
			Power: metalv1alpha1.PowerOff, // we will power on the server later
			ServerSelector: &metav1.LabelSelector{
				MatchLabels:      spec.ServerLabels,
				MatchExpressions: nil,
			},
			IgnitionSecretRef: &corev1.LocalObjectReference{Name: secret.Name},
			Image:             spec.Image,
		},
	}
}

func (d *metalDriver) updateIgnitionAndPowerOnServer(ctx context.Context, req *driver.CreateMachineRequest, serverClaim *metalv1alpha1.ServerClaim, providerSpec *apiv1alpha1.ProviderSpec, addressesMetaData map[string]any, hostname string) error {
	serverMetadata, err := d.extractServerMetadataFromClaim(ctx, serverClaim)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("error extracting server metadata from claim: %s", err.Error()))
	}

	ignitionSecret, err := d.generateIgnition(ctx, req, hostname, providerSpec, addressesMetaData, serverMetadata)
	if err != nil {
		return err
	}

	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client
	if err := metalClient.Patch(ctx, ignitionSecret, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return err
	}

	serverClaimBase := serverClaim.DeepCopy()
	serverClaim.Spec.Power = metalv1alpha1.PowerOn

	if err := metalClient.Patch(ctx, serverClaim, client.MergeFrom(serverClaimBase)); err != nil {
		return err
	}

	return nil
}

// isEmptyCreateRequest checks if any of the fields in CreateMachineRequest is empty
func isEmptyCreateRequest(req *driver.CreateMachineRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}

// getOrCreateIPAddressClaims gets or creates IPAddressClaims for the ipam config
func (d *metalDriver) getOrCreateIPAddressClaims(ctx context.Context, req *driver.CreateMachineRequest, providerSpec *apiv1alpha1.ProviderSpec) ([]*capiv1beta1.IPAddressClaim, map[string]any, error) {
	ipAddressClaims := []*capiv1beta1.IPAddressClaim{}
	addressesMetaData := make(map[string]any)

	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client

	for _, networkRef := range providerSpec.IPAMConfig {
		ipAddrClaimName := fmt.Sprintf("%s-%s", req.Machine.Name, networkRef.MetadataKey)
		if len(ipAddrClaimName) > utilvalidation.DNS1123SubdomainMaxLength {
			klog.Info("IP address claim name is too long, it will be shortened which can cause name collisions", "name", ipAddrClaimName)
			ipAddrClaimName = ipAddrClaimName[:utilvalidation.DNS1123SubdomainMaxLength]
		}

		ipAddrClaimKey := client.ObjectKey{Namespace: d.metalNamespace, Name: ipAddrClaimName}
		ipClaim := &capiv1beta1.IPAddressClaim{}
		if err := metalClient.Get(ctx, ipAddrClaimKey, ipClaim); err != nil && !apierrors.IsNotFound(err) {
			return nil, nil, err
		} else if err == nil {
			klog.V(3).Infof("IP address claim found %s", ipAddrClaimKey.String())
			if ipClaim.Status.AddressRef.Name == "" {
				return nil, nil, fmt.Errorf("IP address claim %q has no IP address reference", ipAddrClaimKey.String())
			}
			if ipClaim.Labels == nil {
				return nil, nil, fmt.Errorf("IP address claim %q has no server claim labels", ipAddrClaimKey.String())
			}
			name, nameExists := ipClaim.Labels[LabelKeyServerClaimName]
			namespace, namespaceExists := ipClaim.Labels[LabelKeyServerClaimNamespace]
			if !nameExists || !namespaceExists {
				return nil, nil, fmt.Errorf("IP address claim %q has no server claim labels", ipAddrClaimKey.String())
			}
			if name != req.Machine.Name || namespace != d.metalNamespace {
				return nil, nil, fmt.Errorf("IP address claim %q's server claim labels don't match. Expected: name: %q, namespace: %q. Actual: name: %q, namespace: %q", ipAddrClaimKey.String(), req.Machine.Name, d.metalNamespace, name, namespace)
			}
		} else if apierrors.IsNotFound(err) {
			if networkRef.IPAMRef == nil {
				return nil, nil, errors.New("ipamRef of an ipamConfig is not set")
			}
			klog.V(3).Info("creating IP address claim", "name", ipAddrClaimKey.String())
			apiGroup := networkRef.IPAMRef.APIGroup
			ipClaim = &capiv1beta1.IPAddressClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ipAddrClaimKey.Name,
					Namespace: ipAddrClaimKey.Namespace,
					Labels: map[string]string{
						LabelKeyServerClaimName:      req.Machine.Name,
						LabelKeyServerClaimNamespace: d.metalNamespace,
					},
				},
				Spec: capiv1beta1.IPAddressClaimSpec{
					PoolRef: corev1.TypedLocalObjectReference{
						APIGroup: &apiGroup,
						Kind:     networkRef.IPAMRef.Kind,
						Name:     networkRef.IPAMRef.Name,
					},
				},
			}
			if err = metalClient.Create(ctx, ipClaim); err != nil {
				return nil, nil, fmt.Errorf("error creating IP: %w", err)
			}

			// Wait for the IP address claim to reach the ready state
			err = wait.PollUntilContextTimeout(
				ctx,
				time.Millisecond*50,
				time.Millisecond*340,
				true,
				func(ctx context.Context) (bool, error) {
					if err := metalClient.Get(ctx, ipAddrClaimKey, ipClaim); err != nil {
						return false, client.IgnoreNotFound(err)
					}
					return ipClaim.Status.AddressRef.Name != "", nil
				},
			)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to wait for IPAddressClaim readiness: %w", err)
			}
		}

		ipAddrKey := client.ObjectKey{Namespace: ipClaim.Namespace, Name: ipClaim.Status.AddressRef.Name}
		ipAddr := &capiv1beta1.IPAddress{}
		if err := metalClient.Get(ctx, ipAddrKey, ipAddr); err != nil {
			return nil, nil, err
		}

		ipAddressClaims = append(ipAddressClaims, ipClaim)
		addressesMetaData[networkRef.MetadataKey] = map[string]any{
			"ip":      ipAddr.Spec.Address,
			"prefix":  ipAddr.Spec.Prefix,
			"gateway": ipAddr.Spec.Gateway,
		}
		klog.V(3).Info("IP address will be added to metadata", "name", ipAddrKey.String())
	}
	klog.V(3).Info("Successfully processed all IPs", "number of ips", len(providerSpec.IPAMConfig))
	return ipAddressClaims, addressesMetaData, nil
}

// generateIgnition creates an ignition file for the machine and stores it in a secret
func (d *metalDriver) generateIgnition(ctx context.Context, req *driver.CreateMachineRequest, hostname string, providerSpec *apiv1alpha1.ProviderSpec, addressesMetaData map[string]any, serverMetadata *ServerMetadata) (*corev1.Secret, error) {
	// Get userData from machine secret
	userData, ok := req.Secret.Data["userData"]
	if !ok {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to find user-data in machine secret %s", client.ObjectKeyFromObject(req.Secret)))
	}

	// Ensure providerSpec.MetaData is a map[string]any
	if providerSpec.Metadata == nil {
		providerSpec.Metadata = make(map[string]any)
	}

	// Merge server metadata into providerSpec.MetaData
	if serverMetadata != nil {
		metadata := map[string]any{}
		if serverMetadata.LoopbackAddress != nil {
			metadata["loopbackAddress"] = serverMetadata.LoopbackAddress.String()
		}
		if err := mergo.Merge(&providerSpec.Metadata, metadata, mergo.WithOverride); err != nil {
			return nil, fmt.Errorf("failed to merge server metadata into providerSpec.MetaData: %w", err)
		}
	}

	// Merge addressesMetaData into providerSpec.MetaData
	if err := mergo.Merge(&providerSpec.Metadata, addressesMetaData, mergo.WithOverride); err != nil {
		return nil, fmt.Errorf("failed to merge addressesMetaData into providerSpec.MetaData: %w", err)
	}

	// Construct ignition file config
	config := &ignition.Config{
		Hostname:         hostname,
		UserData:         string(userData),
		MetaData:         providerSpec.Metadata,
		Ignition:         providerSpec.Ignition,
		DnsServers:       providerSpec.DnsServers,
		IgnitionOverride: providerSpec.IgnitionOverride,
	}
	ignitionContent, err := ignition.File(config)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create ignition file for machine %s: %v", req.Machine.Name, err))
	}

	ignitionData := map[string][]byte{}
	ignitionData["ignition"] = []byte(ignitionContent)
	ignitionSecret := &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.getIgnitionNameForMachine(ctx, req.Machine.Name),
			Namespace: d.metalNamespace,
		},
		Data: ignitionData,
	}

	return ignitionSecret, nil
}

// createServerClaim creates and applies a ServerClaim object with proper ignition data
func (d *metalDriver) createServerClaim(ctx context.Context, claim *metalv1alpha1.ServerClaim, ignitionSecret *corev1.Secret) (*metalv1alpha1.ServerClaim, error) {
	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client

	if err := metalClient.Patch(ctx, claim, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error applying metal machine: %s", err.Error()))
	}
	if err := metalClient.Patch(ctx, ignitionSecret, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error applying ignition secret: %s", err.Error()))
	}
	return claim, nil
}

// waitForServerClaim waits for the ServerClaim to claim a server
func (d *metalDriver) waitForServerClaim(ctx context.Context, claim *metalv1alpha1.ServerClaim) error {
	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client

	err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 3*time.Second, true, func(ctx context.Context) (bool, error) {
		if claim.Spec.ServerRef != nil { // early return if the server ref is already set
			return true, nil
		}
		if err := metalClient.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
			return false, err
		}
		if claim.Spec.ServerRef == nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("error waiting for server claim to claim a server: %s", err.Error()))
	}
	return nil
}

type ServerMetadata struct {
	LoopbackAddress net.IP
}

func (d *metalDriver) extractServerMetadataFromClaim(ctx context.Context, claim *metalv1alpha1.ServerClaim) (*ServerMetadata, error) {
	if claim.Spec.ServerRef == nil {
		return nil, fmt.Errorf("server claim %q does not have a server reference", client.ObjectKeyFromObject(claim))
	}

	server := &metalv1alpha1.Server{}
	if err := d.clientProvider.Client.Get(ctx, client.ObjectKey{Name: claim.Spec.ServerRef.Name}, server); err != nil {
		return nil, fmt.Errorf("failed to get server %q: %w", claim.Spec.ServerRef.Name, err)
	}

	serverMetadata := &ServerMetadata{}

	loopbackAddress, ok := server.Annotations[apiv1alpha1.LoopbackAddressAnnotation]
	if ok {
		addr := net.ParseIP(loopbackAddress)
		if addr != nil {
			serverMetadata.LoopbackAddress = addr
		}
	}

	return serverMetadata, nil
}

// setServerClaimOwnershipToIPAddressClaim sets the owner reference of the IPAddressClaims to the ServerClaim
func (d *metalDriver) setServerClaimOwnershipToIPAddressClaim(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim, IPAddressClaims []*capiv1beta1.IPAddressClaim) error {
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
		IPAddressBase := IPAddressClaim.DeepCopy()
		if err := controllerutil.SetOwnerReference(serverClaim, IPAddressBase, metalClient.Scheme()); err != nil {
			return fmt.Errorf("failed to set OwnerReference: %w", err)
		}
		if err := metalClient.Patch(ctx, IPAddressBase, client.MergeFrom(IPAddressClaim)); err != nil {
			return fmt.Errorf("failed to patch IPAddressClaim: %w", err)
		}
		klog.V(3).Info("Owner reference for IPAddressClaim to ServerClaim was set",
			"IPAddressClaim", client.ObjectKeyFromObject(IPAddressClaim).String(),
			"ServerClaim", client.ObjectKeyFromObject(serverClaim).String())
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
