// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"errors"
	"fmt"

	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GetNodeName(ctx context.Context, policy cmd.NodeNamePolicy, serverClaim *metalv1alpha1.ServerClaim, metalNamespace string, metalClient client.Client) (string, error) {
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
		if err := metalClient.Get(ctx, client.ObjectKey{Namespace: metalNamespace, Name: serverClaim.Spec.ServerRef.Name}, &server); err != nil {
			return "", fmt.Errorf("failed to get server %q: %v", serverClaim.Spec.ServerRef.Name, err)
		}
		if server.Spec.BMCRef == nil {
			return "", fmt.Errorf("server %q does not have a BMC configured", serverClaim.Spec.ServerRef.Name)
		}
		return server.Spec.BMCRef.Name, nil
	}
	return "", fmt.Errorf("unknown node name policy: %s", policy)
}
