// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	_ "github.com/gardener/machine-controller-manager/pkg/util/client/metrics/prometheus" // for client metric registration
	"github.com/gardener/machine-controller-manager/pkg/util/provider/app"
	mcmoptions "github.com/gardener/machine-controller-manager/pkg/util/provider/app/options"
	_ "github.com/gardener/machine-controller-manager/pkg/util/reflector/prometheus" // for reflector metric registration
	_ "github.com/gardener/machine-controller-manager/pkg/util/workqueue/prometheus" // for workqueue metric registration
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/metal"
	"github.com/spf13/pflag"
	"k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	logsv1 "k8s.io/component-base/logs/api/v1"
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	clientProvider, err := metal.NewClientProvider(ctx, KubeconfigPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	drv := metal.NewDriver(clientProvider, CSIDriverName)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	if err := app.Run(s, drv); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func AddExtraFlags(fs *pflag.FlagSet) {
	fs.StringVar(&KubeconfigPath, "metal-kubeconfig", "", "Path to the metal cluster kubeconfig.")
}
