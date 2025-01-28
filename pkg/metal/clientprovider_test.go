// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"context"
	"os"
	"path"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const kubeconfigStr = `apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: bW9kZTogc2V0CmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzoyOC4xMyw0MC42MiA5IDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjQwLjYyLDQzLjMgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo0NS4yLDQ2LjE2IDIgMApnaXRodWIuY29tL2lyb25jb3JlLWRldi9tYWNoaW5lLWNvbnRyb2xsZXItbWFuYWdlci1wcm92aWRlci1pcm9uY29yZS1tZXRhbC9jbWQvbWFjaGluZS1jb250cm9sbGVyL21haW4uZ286NDYuMTYsNDkuMyAyIDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjUxLjIsNTIuMTYgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo1Mi4xNiw1NS4zIDIgMApnaXRodWIuY29tL2lyb25jb3JlLWRldi9tYWNoaW5lLWNvbnRyb2xsZXItbWFuYWdlci1wcm92aWRlci1pcm9uY29yZS1tZXRhbC9jbWQvbWFjaGluZS1jb250cm9sbGVyL21haW4uZ286NTcuMiw1Ny40MCAxIDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjU3LjQwLDYwLjMgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo2My4zOSw2NS4yIDEgMApnaXRodWIu
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
    client-certificate-data: bW9kZTogc2V0CmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzoyOC4xMyw0MC42MiA5IDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjQwLjYyLDQzLjMgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo0NS4yLDQ2LjE2IDIgMApnaXRodWIuY29tL2lyb25jb3JlLWRldi9tYWNoaW5lLWNvbnRyb2xsZXItbWFuYWdlci1wcm92aWRlci1pcm9uY29yZS1tZXRhbC9jbWQvbWFjaGluZS1jb250cm9sbGVyL21haW4uZ286NDYuMTYsNDkuMyAyIDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjUxLjIsNTIuMTYgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo1Mi4xNiw1NS4zIDIgMApnaXRodWIuY29tL2lyb25jb3JlLWRldi9tYWNoaW5lLWNvbnRyb2xsZXItbWFuYWdlci1wcm92aWRlci1pcm9uY29yZS1tZXRhbC9jbWQvbWFjaGluZS1jb250cm9sbGVyL21haW4uZ286NTcuMiw1Ny40MCAxIDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjU3LjQwLDYwLjMgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo2My4zOSw2NS4yIDEgMApnaXRodWIu
    client-key-data: bW9kZTogc2V0CmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzoyOC4xMyw0MC42MiA5IDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjQwLjYyLDQzLjMgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo0NS4yLDQ2LjE2IDIgMApnaXRodWIuY29tL2lyb25jb3JlLWRldi9tYWNoaW5lLWNvbnRyb2xsZXItbWFuYWdlci1wcm92aWRlci1pcm9uY29yZS1tZXRhbC9jbWQvbWFjaGluZS1jb250cm9sbGVyL21haW4uZ286NDYuMTYsNDkuMyAyIDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjUxLjIsNTIuMTYgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo1Mi4xNiw1NS4zIDIgMApnaXRodWIuY29tL2lyb25jb3JlLWRldi9tYWNoaW5lLWNvbnRyb2xsZXItbWFuYWdlci1wcm92aWRlci1pcm9uY29yZS1tZXRhbC9jbWQvbWFjaGluZS1jb250cm9sbGVyL21haW4uZ286NTcuMiw1Ny40MCAxIDAKZ2l0aHViLmNvbS9pcm9uY29yZS1kZXYvbWFjaGluZS1jb250cm9sbGVyLW1hbmFnZXItcHJvdmlkZXItaXJvbmNvcmUtbWV0YWwvY21kL21hY2hpbmUtY29udHJvbGxlci9tYWluLmdvOjU3LjQwLDYwLjMgMiAwCmdpdGh1Yi5jb20vaXJvbmNvcmUtZGV2L21hY2hpbmUtY29udHJvbGxlci1tYW5hZ2VyLXByb3ZpZGVyLWlyb25jb3JlLW1ldGFsL2NtZC9tYWNoaW5lLWNvbnRyb2xsZXIvbWFpbi5nbzo2My4zOSw2NS4yIDEgMApnaXRodWIu
`

func wrap(test func(string, context.Context)) func() {
	return func() {
		dirName, err := os.MkdirTemp("/tmp", "client_provider_test")
		Expect(err).ShouldNot(HaveOccurred())
		defer func() {
			err := os.RemoveAll(dirName)
			Expect(err).ShouldNot(HaveOccurred())
		}()

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		test(dirName, ctx)
	}
}

var _ = Describe("ClientProvider", func() {
	When("kubeconfig file is absent", func() {
		It("returns an error", wrap(func(dirName string, ctx context.Context) {
			_, _, err := NewClientProviderAndNamespace(ctx, path.Join(dirName, "kubeconfig"))
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("failed to read metal kubeconfig"))
		}))

		It("returns an error", wrap(func(dirName string, ctx context.Context) {
			_, _, err := NewClientProviderAndNamespace(ctx, path.Join(dirName, "extraDir", "kubeconfig"))
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("unable to add kubeconfig"))
		}))
	})

	When("kubeconfig file exists but it is empty", func() {
		It("returns an error", wrap(func(dirName string, ctx context.Context) {
			kubeconfig := path.Join(dirName, "kubeconfig")
			Expect(os.WriteFile(kubeconfig, []byte(kubeconfigStr), 0644)).ShouldNot(HaveOccurred())
			// cp, ns, err := NewClientProviderAndNamespace(ctx, path.Join(dirName, "kubeconfig"))
			_, _, err := NewClientProviderAndNamespace(ctx, kubeconfig)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).To(HavePrefix("unable to get metal cluster rest config:"))
		}))
	})
})
