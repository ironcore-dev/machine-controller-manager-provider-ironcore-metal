// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"
	"github.com/imdario/mergo"
	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/ignition"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// applyServerClaim reserves a Server by creating a corresponding ServerClaim object with proper ignition data
func (d *metalDriver) applyServerClaim(ctx context.Context, machine *v1alpha1.Machine, providerSpec *apiv1alpha1.ProviderSpec, ignitionSecret *corev1.Secret) (*metalv1alpha1.ServerClaim, error) {
	serverClaim := &metalv1alpha1.ServerClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: metalv1alpha1.GroupVersion.String(),
			Kind:       "ServerClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      machine.Name,
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

	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client

	if err := metalClient.Patch(ctx, serverClaim, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error applying metal machine: %s", err.Error()))
	}

	if err := metalClient.Patch(ctx, ignitionSecret, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error applying ignition secret: %s", err.Error()))
	}

	return serverClaim, nil
}

// generateIgnitionSecret creates an ignition file for the machine and stores it in a secret
func (d *metalDriver) generateIgnitionSecret(ctx context.Context, machine *v1alpha1.Machine, machineClassSecret *corev1.Secret, providerSpec *apiv1alpha1.ProviderSpec, addressesMetaData map[string]any) (*corev1.Secret, error) {
	// Get userData from machine secret
	userData, ok := machineClassSecret.Data["userData"]
	if !ok {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to find user-data in machine secret %s", client.ObjectKeyFromObject(machineClassSecret)))
	}

	// Ensure providerSpec.MetaData is a map[string]any
	if providerSpec.Metadata == nil {
		providerSpec.Metadata = make(map[string]any)
	}

	// Merge addressesMetaData into providerSpec.MetaData
	if err := mergo.Merge(&providerSpec.Metadata, addressesMetaData, mergo.WithOverride); err != nil {
		return nil, fmt.Errorf("failed to merge addressesMetaData into providerSpec.MetaData: %w", err)
	}

	// Construct ignition file config
	config := &ignition.Config{
		Hostname:         machine.Name,
		UserData:         string(userData),
		MetaData:         providerSpec.Metadata,
		Ignition:         providerSpec.Ignition,
		DnsServers:       providerSpec.DnsServers,
		IgnitionOverride: providerSpec.IgnitionOverride,
	}
	ignitionContent, err := ignition.File(config)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create ignition file for machine %s: %v", machine.Name, err))
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

// getOrCreateIPAddressClaims gets or creates IPAddressClaims for the ipam config
func (d *metalDriver) getOrCreateIPAddressClaims(ctx context.Context, machine *v1alpha1.Machine, providerSpec *apiv1alpha1.ProviderSpec) ([]*capiv1beta1.IPAddressClaim, map[string]any, error) {
	var ipAddressClaims []*capiv1beta1.IPAddressClaim
	addressesMetaData := make(map[string]any)

	d.clientProvider.Lock()
	defer d.clientProvider.Unlock()
	metalClient := d.clientProvider.Client

	for _, networkRef := range providerSpec.IPAMConfig {
		ipAddrClaimName := fmt.Sprintf("%s-%s", machine.Name, networkRef.MetadataKey)
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
			if name != machine.Name || namespace != d.metalNamespace {
				return nil, nil, fmt.Errorf("IP address claim %q's server claim labels don't match. Expected: name: %q, namespace: %q. Actual: name: %q, namespace: %q", ipAddrClaimKey.String(), machine.Name, d.metalNamespace, name, namespace)
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
						LabelKeyServerClaimName:      machine.Name,
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
					if err = metalClient.Get(ctx, ipAddrClaimKey, ipClaim); err != nil && !apierrors.IsNotFound(err) {
						return false, err
					}
					return ipClaim.Status.AddressRef.Name != "", nil
				})
			if err != nil {
				return nil, nil, err
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
	}
	return ipAddressClaims, addressesMetaData, nil
}
