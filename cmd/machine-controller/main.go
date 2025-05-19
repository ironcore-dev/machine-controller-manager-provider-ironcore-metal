// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	_ "github.com/gardener/machine-controller-manager/pkg/util/client/metrics/prometheus" // for client metric registration
	"github.com/gardener/machine-controller-manager/pkg/util/provider/app"
	mcmoptions "github.com/gardener/machine-controller-manager/pkg/util/provider/app/options"
	_ "github.com/gardener/machine-controller-manager/pkg/util/reflector/prometheus" // for reflector metric registration
	_ "github.com/gardener/machine-controller-manager/pkg/util/workqueue/prometheus" // for workqueue metric registration
	mcmclient "github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/client"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal"
	"github.com/spf13/pflag"
	"k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	KubeconfigPath          string
	UseServerNameAsNodeName bool
)

func main() {
	s := mcmoptions.NewMCServer()
	s.AddFlags(pflag.CommandLine)

	logs.AddFlags(pflag.CommandLine)
	AddExtraFlags(pflag.CommandLine)

	flag.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	clientProvider, namespace, err := mcmclient.NewProviderAndNamespace(ctrl.SetupSignalHandler(), KubeconfigPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	drv := metal.NewDriver(clientProvider, namespace, UseServerNameAsNodeName)

	if err := app.Run(s, drv); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func AddExtraFlags(fs *pflag.FlagSet) {
	fs.StringVar(&KubeconfigPath, "metal-kubeconfig", "", "Path to the metal cluster kubeconfig.")
	fs.BoolVar(&UseServerNameAsNodeName, "use-server-name-as-node-name", false, "Use server name as node name instead of server claim name.")
}
