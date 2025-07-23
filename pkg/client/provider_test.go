// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const kubeconfigStr = `apiVersion: v1
clusters:
- cluster:
    server: https://127.0.0.1:123
  name: example-cluster
contexts:
- context:
    cluster: example-cluster
    user: example-user
  name: example-context
current-context: example-context
kind: Config
users:
- name: example-user
  user:
    token: example-token
`

func wrap(test func(string, context.Context)) func() {
	return func() {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		test(GinkgoT().TempDir(), ctx)
	}
}

var _ = Describe("Provider", func() {
	When("kubeconfig file is absent", func() {
		It("returns an error", wrap(func(dirName string, ctx context.Context) {
			_, _, err := NewProviderAndNamespace(ctx, path.Join(dirName, "kubeconfig"))
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("failed to read metal kubeconfig"))
		}))

		It("returns an error", wrap(func(dirName string, ctx context.Context) {
			_, _, err := NewProviderAndNamespace(ctx, path.Join(dirName, "extraDir", "kubeconfig"))
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("unable to add kubeconfig"))
		}))
	})

	When("kubeconfig file exists but it is empty", func() {
		It("returns an error", wrap(func(dirName string, ctx context.Context) {
			kubeconfig := path.Join(dirName, "kubeconfig")
			Expect(os.WriteFile(kubeconfig, []byte{}, 0644)).ShouldNot(HaveOccurred())
			_, _, err := NewProviderAndNamespace(ctx, kubeconfig)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("unable to get metal cluster rest config"))
		}))
	})

	When("kubeconfig file exists with correct content", func() {
		It("returns a default namespace and a client", wrap(func(dirName string, ctx context.Context) {
			kubeconfig := path.Join(dirName, "kubeconfig")
			Expect(os.WriteFile(kubeconfig, []byte(kubeconfigStr), 0644)).ShouldNot(HaveOccurred())
			cp, ns, err := NewProviderAndNamespace(ctx, kubeconfig)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ns).To(Equal("default"))
			Expect(cp).NotTo(BeNil())
		}))

		When("kubeconfig file has changed", func() {
			It("updates the client", wrap(func(dirName string, ctx context.Context) {
				atomicWrite(dirName, "kubeconfig", []byte(kubeconfigStr))

				cp, _, err := NewProviderAndNamespace(ctx, path.Join(dirName, "kubeconfig"))
				Expect(err).ShouldNot(HaveOccurred())

				cp.mu.Lock()
				oldClient := cp.client
				cp.mu.Unlock()

				newKubeconfigStr := strings.Replace(kubeconfigStr, "123", "321", 1)
				atomicWrite(dirName, "kubeconfig", []byte(newKubeconfigStr))

				Eventually(func(g Gomega) {
					cp.mu.Lock()
					newClient := cp.client
					cp.mu.Unlock()
					g.Expect(newClient).NotTo(Equal(oldClient))
				}).Should(Succeed())
			}))
		})
	})
})

// atomicWrite is a function that mimic behaviour of k8s.io/kubernetes/pkg/volume/util AtomicWriter which is the way k8s controllers save mounted files from secrets.
func atomicWrite(targetDir string, fileName string, content []byte) {
	dataDirPath := filepath.Join(targetDir, "..data")
	oldTsDir, _ := os.Readlink(dataDirPath)
	oldTsPath := filepath.Join(targetDir, oldTsDir)

	tsDir, err := os.MkdirTemp(targetDir, time.Now().UTC().Format("..2006_01_02_15_04_05."))
	Expect(err).ShouldNot(HaveOccurred())
	fullPath := filepath.Join(tsDir, fileName)
	Expect(os.WriteFile(fullPath, content, os.ModePerm)).Should(Succeed())

	newDataDirPath := filepath.Join(targetDir, "..data_tmp")
	Expect(os.Symlink(filepath.Base(tsDir), newDataDirPath)).Should(Succeed())
	Expect(os.Rename(newDataDirPath, dataDirPath)).Should(Succeed())

	visibleFile := filepath.Join(targetDir, fileName)
	_, err = os.Readlink(visibleFile)
	if err != nil && os.IsNotExist(err) {
		Expect(os.Symlink(filepath.Join("..data", fileName), visibleFile)).Should(Succeed())
	}

	if len(oldTsDir) > 0 {
		Expect(os.RemoveAll(oldTsPath)).Should(Succeed())
	}
}
