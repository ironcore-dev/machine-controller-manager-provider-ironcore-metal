// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	gardenermachinev1alpha1 "github.com/gardener/machine-controller-manager/pkg/apis/machine/v1alpha1"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/driver"
	"github.com/ironcore-dev/controller-utils/modutils"
	ipamv1alpha1 "github.com/ironcore-dev/ipam/api/ipam/v1alpha1"
	"github.com/ironcore-dev/machine-controller-manager-provider-ironcore-metal/pkg/api/v1alpha1"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	capiv1beta1 "sigs.k8s.io/cluster-api/exp/ipam/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	eventuallyTimeout    = 20 * time.Second
	pollingInterval      = 250 * time.Millisecond
	consistentlyDuration = 1 * time.Second
	apiServiceTimeout    = 5 * time.Minute
)

var (
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

func TestAPIs(t *testing.T) {
	SetDefaultConsistentlyPollingInterval(pollingInterval)
	SetDefaultEventuallyPollingInterval(pollingInterval)
	SetDefaultEventuallyTimeout(eventuallyTimeout)
	SetDefaultConsistentlyDuration(consistentlyDuration)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Machine Controller Manager Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			modutils.Dir("github.com/gardener/machine-controller-manager", "kubernetes", "crds", "machine.sapcloud.io_machineclasses.yaml"),
			modutils.Dir("github.com/gardener/machine-controller-manager", "kubernetes", "crds", "machine.sapcloud.io_machinedeployments.yaml"),
			modutils.Dir("github.com/gardener/machine-controller-manager", "kubernetes", "crds", "machine.sapcloud.io_machines.yaml"),
			modutils.Dir("github.com/gardener/machine-controller-manager", "kubernetes", "crds", "machine.sapcloud.io_machinesets.yaml"),
			modutils.Dir("github.com/ironcore-dev/metal-operator", "config", "crd", "bases"),
			modutils.Dir("github.com/ironcore-dev/ipam", "config", "crd", "bases"),
			modutils.Dir("sigs.k8s.io/cluster-api", "config", "crd", "bases"),
		},
		ErrorIfCRDPathMissing: true,

		// The BinaryAssetsDirectory is only required if you want to run the tests directly
		// without call the makefile target test. If not informed it will look for the
		// default path defined in controller-runtime which is /usr/local/kubebuilder/.
		// Note that you must have the required binaries setup under the bin directory to perform
		// the tests directly. When we run make test it will be setup and used automatically.
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.29.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	DeferCleanup(testEnv.Stop)

	//+kubebuilder:scaffold:scheme
	Expect(metalv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(ipamv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(capiv1beta1.AddToScheme(scheme.Scheme)).To(Succeed())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// set komega client
	SetClient(k8sClient)
})

func SetupTest() (*corev1.Namespace, *corev1.Secret, *driver.Driver) {
	var (
		drv driver.Driver
	)
	ns := &corev1.Namespace{}
	secret := &corev1.Secret{}

	BeforeEach(func(ctx SpecContext) {
		*ns = corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "testns-",
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed(), "failed to create test namespace")
		DeferCleanup(k8sClient.Delete, ns)

		// create kubeconfig which we will use as the provider secret to create our metal machine
		user, err := testEnv.AddUser(envtest.User{
			Name:   "dummy",
			Groups: []string{"system:authenticated", "system:masters"},
		}, nil)
		Expect(err).NotTo(HaveOccurred())

		userCfg := user.Config()
		userClient, err := client.New(userCfg, client.Options{Scheme: scheme.Scheme})
		Expect(err).NotTo(HaveOccurred())

		// create provider secret for the machine creation
		secretData := map[string][]byte{}
		secretData["userData"] = []byte("abcd")

		*secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "machine-secret-",
				Namespace:    ns.Name,
			},
			Data: secretData,
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		drv = NewDriver(userClient, ns.Name, "")
	})

	return ns, secret, &drv
}

func newMachine(namespace *corev1.Namespace, prefix string, setMachineIndex int, annotations map[string]string) *gardenermachinev1alpha1.Machine {
	index := 0

	if setMachineIndex > 0 {
		index = setMachineIndex
	}

	machine := &gardenermachinev1alpha1.Machine{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "machine.sapcloud.io",
			Kind:       "Machine",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace.Name,
			Name:      fmt.Sprintf("%s-%d", prefix, index),
		},
	}

	// Don't initialize providerID and node if setMachineIndex == -1
	if setMachineIndex != -1 {
		machine.Spec = gardenermachinev1alpha1.MachineSpec{
			ProviderID: fmt.Sprintf("%s:///%s/%s-%d", v1alpha1.ProviderName, namespace.Name, prefix, setMachineIndex),
		}
		machine.Labels = map[string]string{
			gardenermachinev1alpha1.NodeLabelKey: fmt.Sprintf("ip-%d", setMachineIndex),
		}
	}

	machine.Spec.NodeTemplateSpec.ObjectMeta.Annotations = make(map[string]string)

	//appending to already existing annotations
	for k, v := range annotations {
		machine.Spec.NodeTemplateSpec.ObjectMeta.Annotations[k] = v
	}
	return machine
}

func newMachineClass(providerName string, providerSpec map[string]interface{}) *gardenermachinev1alpha1.MachineClass {
	providerSpecJSON, err := json.Marshal(providerSpec)
	Expect(err).ShouldNot(HaveOccurred())
	return &gardenermachinev1alpha1.MachineClass{
		ProviderSpec: kuberuntime.RawExtension{
			Raw: providerSpecJSON,
		},
		Provider: providerName,
		NodeTemplate: &gardenermachinev1alpha1.NodeTemplate{
			InstanceType: "foo",
			Region:       "foo",
			Zone:         "az1",
		},
	}
}
