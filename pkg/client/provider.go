// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/scale/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

type syncClientFunc func(client client.Client) error

type Provider struct {
	client         client.Client
	mu             sync.Mutex
	s              *runtime.Scheme
	kubeconfigPath string
}

func NewProviderAndNamespace(ctx context.Context, kubeconfigPath string) (*Provider, string, error) {
	cp := &Provider{s: runtime.NewScheme(), kubeconfigPath: kubeconfigPath}
	utilruntime.Must(scheme.AddToScheme(cp.s))
	utilruntime.Must(corev1.AddToScheme(cp.s))
	utilruntime.Must(metalv1alpha1.AddToScheme(cp.s))
	utilruntime.Must(capiv1beta1.AddToScheme(cp.s))
	ctrllog.SetLogger(klog.NewKlogr())

	if err := cp.reloadMetalClientOnConfigChange(ctx); err != nil {
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

	klog.V(3).Infof("A new client provider was created for %s", kubeconfigPath)
	return cp, namespace, nil
}

func (p *Provider) SyncClient(fn syncClientFunc) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		return fmt.Errorf("client is not initialized")
	}
	return fn(p.client)
}

func (p *Provider) GetClientScheme() *runtime.Scheme {
	return p.client.Scheme()
}

func (p *Provider) SetClient(newClient client.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = newClient
}

func (p *Provider) getClientConfig() (clientcmd.OverridingClientConfig, error) {
	kubeconfigData, err := os.ReadFile(p.kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metal kubeconfig %s: %w", p.kubeconfigPath, err)
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

func (p *Provider) setMetalClient(clientConfig clientcmd.OverridingClientConfig) error {
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return fmt.Errorf("unable to get metal cluster rest config: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	newClient, err := client.New(restConfig, client.Options{Scheme: p.s})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	p.client = newClient
	return nil
}

func (p *Provider) reloadMetalClientOnConfigChange(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("unable to create kubeconfig watcher: %w", err)
	}

	if err = watcher.Add(path.Dir(p.kubeconfigPath)); err != nil {
		watcher.Close()
		return fmt.Errorf("unable to add kubeconfig \"%s\" to watcher: %v", p.kubeconfigPath, err)
	}

	// Because kubeconfig is mounted from a secret and updated by kubernetes it is a symbolic link and
	// there will be no events with kubeconfig name. So we need to check if a target file has changed.
	targetKubeconfigPath, _ := filepath.EvalSymlinks(p.kubeconfigPath)
	go func() {
		defer func() {
			watcher.Close()
			klog.V(3).Infof("Watcher loop ended for %s", path.Dir(p.kubeconfigPath))
		}()
		klog.V(3).Infof("Watcher loop started for %s", path.Dir(p.kubeconfigPath))

		for {
			select {
			case err := <-watcher.Errors:
				klog.Fatalf("Watcher returned an error: %v", err)
			case event := <-watcher.Events:
				klog.V(3).Infof("Event: %s", event.String())
				newTargetKubeconfigPath, _ := filepath.EvalSymlinks(p.kubeconfigPath)
				if newTargetKubeconfigPath == targetKubeconfigPath {
					continue
				}
				targetKubeconfigPath = newTargetKubeconfigPath

				clientConfig, err := p.getClientConfig()
				if err != nil {
					klog.Warningf("Couldn't get client config when config changed: %v", err)
					continue
				}
				if err := p.setMetalClient(clientConfig); err != nil {
					klog.Warningf("Couldn't update metal client when config changed: %v", err)
					continue
				}
				klog.V(3).Infof("Change of kubeconfig was handled successfully")
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
