// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"fmt"

	apiv1alpha1 "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/validation"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"

	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/codes"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/machinecodes/status"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// CreateMachine handles a machine creation request
func (d *metalDriver) CreateMachine(ctx context.Context, req *driver.CreateMachineRequest) (*driver.CreateMachineResponse, error) {
	if isEmptyCreateRequest(req) {
		return nil, status.Error(codes.InvalidArgument, "received empty CreateMachineRequest")
	}

	if req.MachineClass.Provider != apiv1alpha1.ProviderName {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("requested provider %q is not supported by the driver %q", req.MachineClass.Provider, apiv1alpha1.ProviderName))
	}

	klog.V(3).Info("Machine creation request has been received", "name", req.Machine.Name)
	defer klog.V(3).Info("Machine creation request has been processed", "name", req.Machine.Name)

	providerSpec, err := GetProviderSpec(req.MachineClass, req.Secret)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get provider spec: %v", err))
	}

	serverClaim, err := d.createServerClaim(ctx, req, providerSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create ServerClaim: %v", err))
	}

	err = d.createIPAddressClaims(ctx, req, serverClaim, providerSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create IPAddressClaims: %v", err))
	}

	// we need the server to be bound if not the ServerClaimName policy in order to get the node name
	if d.nodeNamePolicy != cmd.NodeNamePolicyServerClaimName {
		serverBound, err := d.ServerIsBound(ctx, serverClaim)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to check if server is bound: %v", err))
		}

		if serverBound {
			klog.V(3).Info("Server is already boun, removing recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace)
			err = d.patchServerClaimWithRecreateAnnotation(ctx, serverClaim, false)
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("failed to patch ServerClaim without recreate annotation: %v", err))
			}
		} else {
			klog.V(3).Info("Server is still not bound, adding recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace)
			err = d.patchServerClaimWithRecreateAnnotation(ctx, serverClaim, true)
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("failed to patch ServerClaim with recreate annotation: %v", err))
			}
			// MCM provider retry with codes.Unavailable will ensure a short retry in 5 seconds
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("server %q in namespace %q is still not bound", req.Machine.Name, d.metalNamespace))
		}
	}

	nodeName, err := getNodeName(ctx, d.nodeNamePolicy, serverClaim, d.metalNamespace, d.clientProvider)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get node name: %v", err))
	}

	return &driver.CreateMachineResponse{
		ProviderID: getProviderIDForServerClaim(serverClaim),
		NodeName:   nodeName,
	}, nil
}

// isEmptyCreateRequest checks if any of the fields in CreateMachineRequest is empty
func isEmptyCreateRequest(req *driver.CreateMachineRequest) bool {
	return req == nil || req.MachineClass == nil || req.Machine == nil || req.Secret == nil
}

// createIPAddressClaims creates IPAddressClaims for the ipam config
func (d *metalDriver) createIPAddressClaims(ctx context.Context, req *driver.CreateMachineRequest, serverClaim *metalv1alpha1.ServerClaim, providerSpec *apiv1alpha1.ProviderSpec) error {
	klog.V(3).Info("Creating IPAddressClaims", "name", req.Machine.Name, "namespace", d.metalNamespace)

	for _, ipamConfig := range providerSpec.IPAMConfig {
		if ipamConfig.IPAMRef == nil {
			return status.Error(codes.Internal, fmt.Sprintf("IPAMRef of an IPAMConfig %q is not set", ipamConfig.MetadataKey))
		}

		ipClaim := &capiv1beta1.IPAddressClaim{
			TypeMeta: metav1.TypeMeta{
				APIVersion: capiv1beta1.GroupVersion.String(),
				Kind:       "IPAddressClaim",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      getIPAddressClaimName(req.Machine.Name, ipamConfig.MetadataKey),
				Namespace: d.metalNamespace,
				Labels: map[string]string{
					validation.LabelKeyServerClaimName:      req.Machine.Name,
					validation.LabelKeyServerClaimNamespace: d.metalNamespace,
				},
			},
			Spec: capiv1beta1.IPAddressClaimSpec{
				PoolRef: corev1.TypedLocalObjectReference{
					APIGroup: ptr.To(ipamConfig.IPAMRef.APIGroup),
					Kind:     ipamConfig.IPAMRef.Kind,
					Name:     ipamConfig.IPAMRef.Name,
				},
			},
		}

		if err := controllerutil.SetOwnerReference(serverClaim, ipClaim, d.clientProvider.GetClientScheme()); err != nil {
			return fmt.Errorf("failed to set owner reference for IPAddressClaim %q: %v", ipClaim.Name, err)
		}

		if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
			return metalClient.Patch(ctx, ipClaim, client.Apply, fieldOwner, client.ForceOwnership)
		}); err != nil {
			return fmt.Errorf("failed to create IPAddressClaim: %s", err.Error())
		}
	}

	klog.V(3).Info("Successfully created all IPAddressClaims", "count", len(providerSpec.IPAMConfig))
	return nil
}

// createServerClaim creates and applies a ServerClaim object with proper ignition data
func (d *metalDriver) createServerClaim(ctx context.Context, req *driver.CreateMachineRequest, providerSpec *apiv1alpha1.ProviderSpec) (*metalv1alpha1.ServerClaim, error) {
	klog.V(3).Info("Creating ServerClaim", "name", req.Machine.Name, "namespace", d.metalNamespace)

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
			Power: metalv1alpha1.PowerOff, // we will power on the server later
			ServerSelector: &metav1.LabelSelector{
				MatchLabels:      providerSpec.ServerLabels,
				MatchExpressions: nil,
			},
			Image: providerSpec.Image,
		},
	}

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Patch(ctx, serverClaim, client.Apply, fieldOwner, client.ForceOwnership)
	}); err != nil {
		return nil, fmt.Errorf("failed to create ServerClaim: %s", err.Error())
	}

	klog.V(3).Info("Successfully created ServerClaim", "name", serverClaim.Name, "namespace", serverClaim.Namespace)
	return serverClaim, nil
}

// patchServerClaimWithRecreateAnnotation patches the ServerClaim with an annotation to trigger a machine recreation
func (d *metalDriver) patchServerClaimWithRecreateAnnotation(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim, addAnnotation bool) error {
	klog.V(3).Info("Patching ServerClaim with recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace, "addAnnotation", addAnnotation)

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		baseServerClaim := serverClaim.DeepCopy()
		if addAnnotation {
			if serverClaim.Annotations == nil {
				serverClaim.Annotations = make(map[string]string)
			}
			serverClaim.Annotations[validation.AnnotationKeyMCMMachineRecreate] = "true"
		} else {
			delete(serverClaim.Annotations, validation.AnnotationKeyMCMMachineRecreate)
		}
		return metalClient.Patch(ctx, serverClaim, client.MergeFrom(baseServerClaim))
	}); err != nil {
		return fmt.Errorf("failed to patch ServerClaim: %s", err.Error())
	}

	return nil
}

// ServerIsBound checks if the server is already bound
func (d *metalDriver) ServerIsBound(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim) (bool, error) {
	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Get(ctx, client.ObjectKeyFromObject(serverClaim), serverClaim)
	}); err != nil {
		return false, fmt.Errorf("failed to get ServerClaim %q: %v", serverClaim.Name, err)
	}

	return serverClaim.Spec.ServerRef != nil, nil
}
