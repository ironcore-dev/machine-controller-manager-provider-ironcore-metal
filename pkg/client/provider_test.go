// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"os"
	"path"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
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
				aw, err := volumeutil.NewAtomicWriter(dirName, "test")
				Expect(err).ShouldNot(HaveOccurred())
				err = aw.Write(map[string]volumeutil.FileProjection{"kubeconfig": {Data: []byte(kubeconfigStr), Mode: 0644}}, nil)
				Expect(err).ShouldNot(HaveOccurred())

				cp, _, err := NewProviderAndNamespace(ctx, path.Join(dirName, "kubeconfig"))
				Expect(err).ShouldNot(HaveOccurred())

				cp.mu.Lock()
				oldClient := cp.Client
				cp.mu.Unlock()

				newKubeconfigStr := strings.Replace(kubeconfigStr, "123", "321", 1)
				err = aw.Write(map[string]volumeutil.FileProjection{"kubeconfig": {Data: []byte(newKubeconfigStr), Mode: 0644}}, nil)
				Expect(err).ShouldNot(HaveOccurred())

				Eventually(func(g Gomega) {
					cp.mu.Lock()
					newClient := cp.Client
					cp.mu.Unlock()
					g.Expect(newClient).NotTo(Equal(oldClient))
				}).Should(Succeed())
			}))
		})
	})
})
