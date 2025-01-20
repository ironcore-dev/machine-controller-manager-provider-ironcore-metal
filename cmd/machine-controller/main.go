// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/gardener/machine-controller-manager/pkg/client/clientset/versioned/scheme"
	_ "github.com/gardener/machine-controller-manager/pkg/util/client/metrics/prometheus" // for client metric registration
	"github.com/gardener/machine-controller-manager/pkg/util/provider/app"
	mcmoptions "github.com/gardener/machine-controller-manager/pkg/util/provider/app/options"
	_ "github.com/gardener/machine-controller-manager/pkg/util/reflector/prometheus" // for reflector metric registration
	_ "github.com/gardener/machine-controller-manager/pkg/util/workqueue/prometheus" // for workqueue metric registration
	ipamv1alpha1 "github.com/ironcore-dev/ipam/api/ipam/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	logsv1 "k8s.io/component-base/logs/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	KubeconfigPath string
	CSIDriverName  string
)

func main() {
	s := mcmoptions.NewMCServer()
	s.AddFlags(pflag.CommandLine)

	options := logs.NewOptions()
	logs.AddFlags(pflag.CommandLine)
	AddExtraFlags(pflag.CommandLine)

	flag.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	if err := logsv1.ValidateAndApply(options, nil); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	metalClient, namespace, err := getMetalClientAndNamespace()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	drv := metal.NewDriver(&metal.ClientProvider{Client: metalClient}, namespace, CSIDriverName)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := app.Run(s, drv); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func getMetalClientAndNamespace() (client.Client, string, error) {
	s := runtime.NewScheme()
	utilruntime.Must(scheme.AddToScheme(s))
	utilruntime.Must(corev1.AddToScheme(s))
	utilruntime.Must(metalv1alpha1.AddToScheme(s))
	utilruntime.Must(ipamv1alpha1.AddToScheme(s))

	kubeconfigData, err := os.ReadFile(KubeconfigPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read metal kubeconfig %s: %w", KubeconfigPath, err)
	}
	kubeconfig, err := clientcmd.Load(kubeconfigData)
	if err != nil {
		return nil, "", fmt.Errorf("unable to read metal cluster kubeconfig: %w", err)
	}
	clientConfig := clientcmd.NewDefaultClientConfig(*kubeconfig, nil)
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("unable to get metal cluster rest config: %w", err)
	}
	namespace, _, err := clientConfig.Namespace()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get namespace from metal cluster kubeconfig: %w", err)
	}
	if namespace == "" {
		return nil, "", fmt.Errorf("got a empty namespace from metal cluster kubeconfig")
	}
	c, err := client.New(restConfig, client.Options{Scheme: s})
	if err != nil {
		return nil, "", fmt.Errorf("failed to create client: %w", err)
	}
	return c, namespace, nil
}

func AddExtraFlags(fs *pflag.FlagSet) {
	fs.StringVar(&KubeconfigPath, "metal-kubeconfig", "", "Path to the metal cluster kubeconfig.")
}
