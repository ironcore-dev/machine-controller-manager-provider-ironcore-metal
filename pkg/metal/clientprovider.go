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
	mu             sync.Mutex
	s              *runtime.Scheme
	kubeconfigPath string
}

func NewClientProviderAndNamespace(ctx context.Context, kubeconfigPath string) (*ClientProvider, string, error) {
	cp := &ClientProvider{s: runtime.NewScheme(), kubeconfigPath: kubeconfigPath}
	utilruntime.Must(scheme.AddToScheme(cp.s))
	utilruntime.Must(corev1.AddToScheme(cp.s))
	utilruntime.Must(metalv1alpha1.AddToScheme(cp.s))
	utilruntime.Must(ipamv1alpha1.AddToScheme(cp.s))

	if err := cp.setMetalClientWhenConfigIsChanged(ctx); err != nil {
		return nil, "", err
	}

	clientConfig, err := cp.getClientConfig()
	if err != nil {
		return nil, "", err
	} else if err := cp.setMetalClient(clientConfig); err != nil {
		return nil, "", err
	}
	namespace, err := getNamespace(clientConfig)
	if err != nil {
		return nil, "", err
	}

	return cp, namespace, nil
}

func (cp *ClientProvider) Lock() {
	cp.mu.Lock()
}

func (cp *ClientProvider) Unlock() {
	cp.mu.Unlock()
}

func (cp *ClientProvider) getClientConfig() (clientcmd.OverridingClientConfig, error) {
	kubeconfigData, err := os.ReadFile(cp.kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metal kubeconfig %s: %w", cp.kubeconfigPath, err)
	}
	kubeconfig, err := clientcmd.Load(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("unable to read metal cluster kubeconfig: %w", err)
	}
	return clientcmd.NewDefaultClientConfig(*kubeconfig, nil), nil
}

func getNamespace(clientConfig clientcmd.OverridingClientConfig) (string, error) {
	namespace, _, err := clientConfig.Namespace()
	if err != nil {
		return "", fmt.Errorf("failed to get namespace from metal cluster kubeconfig: %w", err)
	}
	if namespace == "" {
		return "", fmt.Errorf("got a empty namespace from metal cluster kubeconfig")
	}
	return namespace, nil
}

func (cp *ClientProvider) setMetalClient(clientConfig clientcmd.OverridingClientConfig) error {
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("unable to get metal cluster rest config: %w", err)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.Client, err = client.New(restConfig, client.Options{Scheme: cp.s}); err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	return nil
}

func (cp *ClientProvider) setMetalClientWhenConfigIsChanged(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("unable to create kubeconfig watcher: %w", err)
	}
	err = watcher.Add(path.Dir(cp.kubeconfigPath))
	if err != nil {
		watcher.Close()
		return fmt.Errorf("unable to add kubeconfig \"%s\" to watcher: %v", cp.kubeconfigPath, err)
	}
	go func() {
		defer watcher.Close()
		for {
			select {
			case err := <-watcher.Errors:
				log.Fatalf("watcher returned an error: %v", err)
			case event := <-watcher.Events:
				if event.Name != cp.kubeconfigPath {
					continue
				}

				if clientConfig, err := cp.getClientConfig(); err != nil {
					log.Fatalf("couldn't get client config when config has changed %v", err)
				} else if err := cp.setMetalClient(clientConfig); err != nil {
					log.Fatalf("couldn't update metal client when config has changed %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
