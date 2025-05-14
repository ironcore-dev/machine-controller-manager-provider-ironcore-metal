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

	// we need the server to be bound if not the ServerClaimName policy in order to get the node name
	if d.nodeNamePolicy != cmd.NodeNamePolicyServerClaimName {
		serverBound, err := d.ServerIsBound(ctx, serverClaim)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to check if server is bound: %v", err))
		}

		if serverBound {
			klog.V(3).Info("Server is already bound, removing recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace)
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

	if d.nodeExistsByName(ctx, nodeName) {
		klog.V(3).Info("Node with the same name already exists in cluster, MCM may terminate and recreate machine", "name", nodeName)
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
	klog.V(3).Info("Patching ServerClaim with/-out recreate annotation", "name", serverClaim.Name, "namespace", serverClaim.Namespace, "addAnnotation", addAnnotation)

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
