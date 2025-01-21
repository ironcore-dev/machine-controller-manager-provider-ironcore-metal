// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	"github.com/fsnotify/fsnotify"
	ipamv1alpha1 "github.com/ironcore-dev/ipam/api/ipam/v1alpha1"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/scale/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClientProvider struct {
	Client         client.Client
	Namespace      string
	mu             sync.Mutex
	s              *runtime.Scheme
	kubeconfigPath string
}

func NewClientProvider(ctx context.Context, kubeconfigPath string) (*ClientProvider, error) {
	c := &ClientProvider{s: runtime.NewScheme(), kubeconfigPath: kubeconfigPath}
	utilruntime.Must(scheme.AddToScheme(c.s))
	utilruntime.Must(corev1.AddToScheme(c.s))
	utilruntime.Must(metalv1alpha1.AddToScheme(c.s))
	utilruntime.Must(ipamv1alpha1.AddToScheme(c.s))

	if err := c.setMetalClientAndNamespaceWhenConfigIsChanged(ctx); err != nil {
		return nil, err
	}
	if err := c.setMetalClientAndNamespace(); err != nil {
		return nil, err
	}
	return c, nil
}

func (cp *ClientProvider) Lock() {
	cp.mu.Lock()
}

func (cp *ClientProvider) Unlock() {
	cp.mu.Unlock()
}

func (c *ClientProvider) setMetalClientAndNamespace() error {
	kubeconfigData, err := os.ReadFile(c.kubeconfigPath)
	if err != nil {
		return fmt.Errorf("failed to read metal kubeconfig %s: %w", c.kubeconfigPath, err)
	}
	kubeconfig, err := clientcmd.Load(kubeconfigData)
	if err != nil {
		return fmt.Errorf("unable to read metal cluster kubeconfig: %w", err)
	}
	clientConfig := clientcmd.NewDefaultClientConfig(*kubeconfig, nil)
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("unable to get metal cluster rest config: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Namespace, _, err = clientConfig.Namespace(); err != nil {
		return fmt.Errorf("failed to get namespace from metal cluster kubeconfig: %w", err)
	}
	if c.Namespace == "" {
		return fmt.Errorf("got a empty namespace from metal cluster kubeconfig")
	}
	if c.Client, err = client.New(restConfig, client.Options{Scheme: c.s}); err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	return nil
}

func (c *ClientProvider) setMetalClientAndNamespaceWhenConfigIsChanged(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("unable to create kubeconfig watcher: %w", err)
	}
	err = watcher.Add(path.Dir(c.kubeconfigPath))
	if err != nil {
		watcher.Close()
		return fmt.Errorf("unable to add kubeconfig \"%s\" to watcher: %v", c.kubeconfigPath, err)
	}
	go func() {
		defer watcher.Close()
		for {
			select {
			case err := <-watcher.Errors:
				log.Fatalf("watcher returned an error: %v", err)
			case event := <-watcher.Events:
				if event.Name != c.kubeconfigPath {
					continue
				}
				if err := c.setMetalClientAndNamespace(); err != nil {
					log.Fatalf("couldn't update metal client when config has changed %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
