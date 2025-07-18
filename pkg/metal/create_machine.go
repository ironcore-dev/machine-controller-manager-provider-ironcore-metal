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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	klog.V(3).Info("machine creation request has been received", "name", req.Machine.Name)
	defer klog.V(3).Info("machine creation request has been processed", "name", req.Machine.Name)

	providerSpec, err := GetProviderSpec(req.MachineClass, req.Secret)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get provider spec: %v", err))
	}

	addressClaims, err := d.createIPAddressClaims(ctx, req, providerSpec)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create IPAddressClaims: %v", err))
	}

	serverClaim := d.generateServerClaim(req, providerSpec)
	err = d.createServerClaim(ctx, serverClaim)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create ServerClaim: %v", err))
	}

	if err := d.updateServerClaimOwnershipToIPAddressClaim(ctx, serverClaim, addressClaims); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to update ownership of IPAddressClaims to ServerClaim: %v", err))
	}

	// we need the server to be bound if not the ServerClaimName policy in order to get the node name
	if d.nodeNamePolicy != cmd.NodeNamePolicyServerClaimName {
		serverBound, err := d.ServerIsBound(ctx, serverClaim)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to check if server is bound: %v", err))
		}

		if !serverBound {
			klog.V(3).Info("server is still not bound, patching ServerClaim with recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace)
			err = d.patchServerClaimWithRecreateAnnotation(ctx, serverClaim, true)
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("failed to patch ServerClaim with recreate annotation: %v", err))
			}
			// workaround: codes.Unavailable will ensure a short retry in 5 seconds
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("server %q in namespace %q is still not bound", req.Machine.Name, d.metalNamespace))
		} else {
			err = d.patchServerClaimWithRecreateAnnotation(ctx, serverClaim, false)
			if err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("failed to patch ServerClaim without recreate annotation: %v", err))
			}
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

// createIPAddressClaims creates IPAddressClaims for the ipam config if missing
func (d *metalDriver) createIPAddressClaims(ctx context.Context, req *driver.CreateMachineRequest, providerSpec *apiv1alpha1.ProviderSpec) ([]*capiv1beta1.IPAddressClaim, error) {
	ipAddressClaims := []*capiv1beta1.IPAddressClaim{}

	for _, ipamConfig := range providerSpec.IPAMConfig {
		ipAddrClaimName := getIPAddressClaimName(req.Machine.Name, ipamConfig.MetadataKey)
		ipClaim := &capiv1beta1.IPAddressClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ipAddrClaimName,
				Namespace: d.metalNamespace,
			},
		}

		err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
			return metalClient.Get(ctx, client.ObjectKeyFromObject(ipClaim), ipClaim)
		})

		if err != nil {
			if apierrors.IsNotFound(err) {
				if ipClaim, err = d.createIPAddressClaim(ctx, &ipamConfig, req.Machine.Name, ipClaim.Name); err != nil {
					return nil, err
				}
				klog.V(3).Info("IPAddressClaim created", "namespace", ipClaim.Namespace, "name", ipClaim.Name)
			} else {
				return nil, err
			}
		} else {
			klog.V(3).Info("IPAddressClaim already exists", "namespace", ipClaim.Namespace, "name", ipClaim.Name)
		}

		ipAddressClaims = append(ipAddressClaims, ipClaim)
	}

	klog.V(3).Info("Successfully created all IPAddressClaims", "count", len(providerSpec.IPAMConfig))
	return ipAddressClaims, nil
}

// generateIPAddressClaim generates an IPAddressClaim object based on the IPAMConfig and machine name
func (d *metalDriver) generateIPAddressClaim(ipamConfig *apiv1alpha1.IPAMConfig, machineName string, ipAddrClaimName string) *capiv1beta1.IPAddressClaim {
	return &capiv1beta1.IPAddressClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ipAddrClaimName,
			Namespace: d.metalNamespace,
			Labels: map[string]string{
				validation.LabelKeyServerClaimName:      machineName,
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
}

// createIPAddressClaim creates IPAddressClaim
func (d *metalDriver) createIPAddressClaim(ctx context.Context, ipamConfig *apiv1alpha1.IPAMConfig, machineName string, ipAddrClaimName string) (*capiv1beta1.IPAddressClaim, error) {
	if ipamConfig.IPAMRef == nil {
		return nil, fmt.Errorf("IPAMRef of an IPAMConfig %q is not set", ipamConfig.MetadataKey)
	}

	klog.V(3).Info("creating IP address claim", "name", ipAddrClaimName)

	ipClaim := d.generateIPAddressClaim(ipamConfig, machineName, ipAddrClaimName)

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Create(ctx, ipClaim)
	}); err != nil {
		return nil, fmt.Errorf("failed to create IPAddressClaim: %w", err)
	}

	return ipClaim, nil
}

// generateServerClaim creates a ServerClaim object based on the request and provider spec
func (d *metalDriver) generateServerClaim(req *driver.CreateMachineRequest, spec *apiv1alpha1.ProviderSpec) *metalv1alpha1.ServerClaim {
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
			Image: spec.Image,
		},
	}
}

// createServerClaim creates and applies a ServerClaim object with proper ignition data
func (d *metalDriver) createServerClaim(ctx context.Context, claim *metalv1alpha1.ServerClaim) error {
	klog.V(3).Info("creating ServerClaim", "name", claim.Name, "namespace", claim.Namespace)

	if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
		return metalClient.Patch(ctx, claim, client.Apply, fieldOwner, client.ForceOwnership)
	}); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("failed to create ServerClaim: %s", err.Error()))
	}

	return nil
}

// patchServerClaimWithRecreateAnnotation patches the ServerClaim with an annotation to trigger a machine recreation
func (d *metalDriver) patchServerClaimWithRecreateAnnotation(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim, addAnnotation bool) error {
	klog.V(3).Info("patching ServerClaim with recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace)

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
		return status.Error(codes.Internal, fmt.Sprintf("failed to create ServerClaim: %s", err.Error()))
	}

	return nil
}

// updateServerClaimOwnershipToIPAddressClaim sets the owner reference of the IPAddressClaims to the ServerClaim
func (d *metalDriver) updateServerClaimOwnershipToIPAddressClaim(ctx context.Context, serverClaim *metalv1alpha1.ServerClaim, ipAddressClaims []*capiv1beta1.IPAddressClaim) error {
	klog.V(3).Info("setting owner reference for IPAddressClaims to ServerClaim", "name", client.ObjectKeyFromObject(serverClaim))

	for _, ipAddressClaim := range ipAddressClaims {
		ipAddressBase := ipAddressClaim.DeepCopy()
		if err := controllerutil.SetOwnerReference(serverClaim, ipAddressClaim, d.clientProvider.GetClientScheme()); err != nil {
			return fmt.Errorf("failed to set OwnerReference: %w", err)
		}

		if err := d.clientProvider.SyncClient(func(metalClient client.Client) error {
			return metalClient.Patch(ctx, ipAddressClaim, client.MergeFrom(ipAddressBase))
		}); err != nil {
			return fmt.Errorf("failed to patch IPAddressClaim: %w", err)
		}

		klog.V(3).Info("owner reference for IPAddressClaim to ServerClaim was set",
			"IPAddressClaim", client.ObjectKeyFromObject(ipAddressClaim).String(),
			"ServerClaim", client.ObjectKeyFromObject(serverClaim).String())
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
