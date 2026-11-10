// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"

	machinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	_ "github.com/gardener/machine-controller-manager/pkg/util/client/metrics/prometheus" // for client metric registration
	"github.com/gardener/machine-controller-manager/pkg/util/provider/app"
	mcmoptions "github.com/gardener/machine-controller-manager/pkg/util/provider/app/options"
	_ "github.com/gardener/machine-controller-manager/pkg/util/reflector/prometheus" // for reflector metric registration
	_ "github.com/gardener/machine-controller-manager/pkg/util/workqueue/prometheus" // for workqueue metric registration
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/cmd"
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	KubeconfigPath string
	nodeNamePolicy cmd.NodeNamePolicy = cmd.NodeNamePolicyServerClaimName

	controlScheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(machinev1alpha1.AddToScheme(controlScheme))
}

func main() {
	s := mcmoptions.NewMCServer()
	s.AddFlags(pflag.CommandLine)

	logs.AddFlags(pflag.CommandLine)
	AddExtraFlags(pflag.CommandLine)

	flag.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	ctx := ctrl.SetupSignalHandler()

	clientProvider, namespace, err := mcmclient.NewProviderAndNamespace(ctx, KubeconfigPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := startMachineReconciler(ctx, s, clientProvider, namespace); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to start machine reconciler: %v\n", err)
		os.Exit(1)
	}

	drv := metal.NewDriver(clientProvider, namespace, nodeNamePolicy)

	if err := app.Run(s, drv); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

// startMachineReconciler creates a controller-runtime Manager for the MCM control cluster and registers
// the MachineReconciler with it. The manager is started in a background goroutine.
// It reuses MCM's --control-kubeconfig and --namespace flags so no additional flags are required.
func startMachineReconciler(ctx context.Context, s *mcmoptions.MCServer, clientProvider *mcmclient.Provider, metalNamespace string) error {
	restCfg, err := buildControlConfig(s.ControlKubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build control cluster config: %w", err)
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme: controlScheme,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{s.Namespace: {}},
		},
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		return fmt.Errorf("failed to create control cluster manager: %w", err)
	}

	reconciler := metal.NewMachineReconciler(mgr.GetClient(), clientProvider, metalNamespace)
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed to setup MachineReconciler: %w", err)
	}

	klog.V(3).InfoS("Starting MachineReconciler manager", "namespace", s.Namespace)
	go func() {
		if err := mgr.Start(ctx); err != nil {
			klog.Fatalf("MachineReconciler manager failed: %v", err)
		}
	}()
	return nil
}

// buildControlConfig resolves the control cluster rest.Config the same way MCM's app.Run does:
// empty or "inClusterConfig" → in-cluster config, anything else → file-based kubeconfig.
func buildControlConfig(controlKubeconfig string) (*rest.Config, error) {
	if controlKubeconfig == "" || controlKubeconfig == "inClusterConfig" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load in-cluster config: %w", err)
		}
		return cfg, nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", controlKubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load control kubeconfig %q: %w", controlKubeconfig, err)
	}
	return cfg, nil
}

func AddExtraFlags(fs *pflag.FlagSet) {
	fs.StringVar(&KubeconfigPath, "metal-kubeconfig", "", "Path to the metal cluster kubeconfig.")
	fs.Var(&nodeNamePolicy, "node-name-policy", fmt.Sprintf("Define the node name policy. Possible values are '%s', '%s' and '%s'.", cmd.NodeNamePolicyBMCName, cmd.NodeNamePolicyServerName, cmd.NodeNamePolicyServerClaimName))
}
