// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/imdario/mergo"
	ipamv1alpha1 "github.com/ironcore-dev/ipam/api/ipam/v1alpha1"
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
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	addressMetaData, err := d.applyIPAddresses(ctx, req, providerSpec)
	if err != nil {
		return nil, err
	}

	ignitionSecret, err := d.applyIgnition(ctx, req, providerSpec, addressMetaData)
	if err != nil {
		return nil, err
	}

	serverClaim, err := d.applyServerClaim(ctx, req, providerSpec, ignitionSecret)
	if err != nil {
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

// applyIPAddresses creates IPAddresses for the machine and stores them in a secret
func (d *metalDriver) applyIPAddresses(ctx context.Context, req *driver.CreateMachineRequest, providerSpec *apiv1alpha1.ProviderSpec) ([]map[string]any, error) {
	var allAddressMetaData []map[string]any

	for _, networkRef := range providerSpec.IPAMConfig {
		ipAddrName := req.Machine.Name

		switch networkRef.IPAMRef.APIGroup {
		case ipamv1alpha1.SchemeGroupVersion.Group:
			// check if IPAddress exists
			ipAddr := &ipamv1alpha1.IP{}
			ipAddrKey := apitypes.NamespacedName{
				Namespace: d.metalNamespace,
				Name:      ipAddrName,
			}
			var err error
			if err = d.metalClient.Get(ctx, ipAddrKey, ipAddr); err != nil && !apierrors.IsNotFound(err) {
				return nil, err
			}
			if err == nil {
				klog.V(3).Infof("IP found %s", ipAddrName)
			}
			if apierrors.IsNotFound(err) {
				if networkRef.IPAMRef == nil {
					return nil, errors.New("ipamRef of an ipamConfig is not set")
				}
				klog.V(3).Infof("creating IP to claim address %s", ipAddrName)
				subnetRef := corev1.LocalObjectReference{Name: networkRef.IPAMRef.Name}
				ip := &ipamv1alpha1.IP{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ipAddrName,
						Namespace: d.metalNamespace,
					},
					Spec: ipamv1alpha1.IPSpec{Subnet: subnetRef},
				}
				if err = d.metalClient.Create(ctx, ip); err != nil {
					return nil, fmt.Errorf("error applying IP: %w", err)
				}
				// Wait for the IP address to reach the finished state
				err = wait.PollUntilContextTimeout(
					ctx,
					time.Millisecond*50,
					time.Millisecond*340,
					true,
					func(ctx context.Context) (bool, error) {
						ipAddr.Status.State, err = ipamv1alpha1.CFinishedIPState, nil
						if err != nil {
							return false, nil
						}
						return true, nil
					})
				if err != nil {
					return nil, fmt.Errorf("failed to wait for for ip to be finished: %w", err)
				}
			}

			// TODO: add net.IP validation
			addressMetaData := map[string]any{
				networkRef.MetadataKey: map[string]any{
					"ip": ipAddr.Status.Reserved.Net.String(),
				},
			}
			allAddressMetaData = append(allAddressMetaData, addressMetaData)
		case capiv1beta1.GroupVersion.Group:
			ipClaim := &capiv1beta1.IPAddressClaim{}
			ipClaimKey := apitypes.NamespacedName{
				Namespace: d.metalNamespace,
				Name:      ipAddrName,
			}
			var err error
			if err = d.metalClient.Get(ctx, ipClaimKey, ipClaim); err != nil && !apierrors.IsNotFound(err) {
				return nil, err
			}
			if err == nil {
				klog.V(3).Infof("IP found %s", ipAddrName)
				if !isIPAddressClaimReady(ipClaim) {
					return nil, errors.New("IP address claim isn't ready")
				}
			}
			if apierrors.IsNotFound(err) {
				if networkRef.IPAMRef == nil {
					return nil, errors.New("ipamRef of an ipamConfig is not set")
				}
				klog.V(3).Infof("creating IP to claim address %s", ipAddrName)
				apiGroup := capiv1beta1.GroupVersion.Group
				ipClaim = &capiv1beta1.IPAddressClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ipAddrName,
						Namespace: d.metalNamespace,
					},
					Spec: capiv1beta1.IPAddressClaimSpec{
						PoolRef: corev1.TypedLocalObjectReference{
							APIGroup: &apiGroup,
							Kind:     "IPAddressClaim",
							Name:     networkRef.IPAMRef.Name,
						},
					},
				}
				if err = d.metalClient.Create(ctx, ipClaim); err != nil {
					return nil, fmt.Errorf("error creating IP: %w", err)
				}
				// Wait for the IP address to reach the ready state
				err = wait.PollUntilContextTimeout(
					ctx,
					time.Millisecond*50,
					time.Millisecond*340,
					true,
					func(ctx context.Context) (bool, error) {
						if err = d.metalClient.Get(ctx, ipClaimKey, ipClaim); err != nil && !apierrors.IsNotFound(err) {
							return false, err
						}
						return isIPAddressClaimReady(ipClaim), nil
					})
			}

			ipAddrKeya := apitypes.NamespacedName{
				Namespace: d.metalNamespace,
				Name:      ipAddrName,
			}
			ipAddr := &capiv1beta1.IPAddress{}
			if err := d.metalClient.Get(ctx, ipAddrKeya, ipAddr); err != nil {
				return nil, err
			}

			addressMetaData := map[string]any{
				networkRef.MetadataKey: map[string]any{
					"ip": ipAddr.Spec.Address,
				},
			}
			allAddressMetaData = append(allAddressMetaData, addressMetaData)
		}
	}
	return allAddressMetaData, nil
}

func isIPAddressClaimReady(ipClaim *capiv1beta1.IPAddressClaim) bool {
	for _, cnd := range ipClaim.GetConditions() {
		if cnd.Type == "Ready" && cnd.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// applyIgnition creates an ignition file for the machine and stores it in a secret
func (d *metalDriver) applyIgnition(ctx context.Context, req *driver.CreateMachineRequest, providerSpec *apiv1alpha1.ProviderSpec, addressMetaData []map[string]any) (*corev1.Secret, error) {
	// Get userData from machine secret
	userData, ok := req.Secret.Data["userData"]
	if !ok {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to find user-data in machine secret %s", client.ObjectKeyFromObject(req.Secret)))
	}

	// Ensure providerSpec.MetaData is a map[string]any
	if providerSpec.Metadata == nil {
		providerSpec.Metadata = make(map[string]any)
	}

	// Merge addressMetaData into providerSpec.MetaData
	for _, metaData := range addressMetaData {
		if err := mergo.Merge(&providerSpec.Metadata, metaData, mergo.WithOverride); err != nil {
			return nil, fmt.Errorf("failed to merge addressMetaData into providerSpec.MetaData: %w", err)
		}
	}

	// Construct ignition file config
	config := &ignition.Config{
		Hostname:         req.Machine.Name,
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

// applyServerClaim reserves a Server by creating corresponding ServerClaim object with proper ignition data
func (d *metalDriver) applyServerClaim(ctx context.Context, req *driver.CreateMachineRequest, providerSpec *apiv1alpha1.ProviderSpec, ignitionSecret *corev1.Secret) (*metalv1alpha1.ServerClaim, error) {
	serverClaim := &metalv1alpha1.ServerClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: metalv1alpha1.GroupVersion.String(),
			Kind:       "ServerClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Machine.Name,
			Namespace: d.metalNamespace,
			Labels:    providerSpec.Labels,
		},
		Spec: metalv1alpha1.ServerClaimSpec{
			Power: "On",
			ServerSelector: &metav1.LabelSelector{
				MatchLabels:      providerSpec.ServerLabels,
				MatchExpressions: nil,
			},
			IgnitionSecretRef: &corev1.LocalObjectReference{Name: ignitionSecret.Name},
			Image:             providerSpec.Image,
		},
	}

	if err := d.metalClient.Patch(ctx, serverClaim, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error applying metal machine: %s", err.Error()))
	}

	if err := d.metalClient.Patch(ctx, ignitionSecret, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error applying ignition secret: %s", err.Error()))
	}

	return serverClaim, nil
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
