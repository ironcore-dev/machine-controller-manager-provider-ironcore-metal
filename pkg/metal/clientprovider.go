// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package metal

import (
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClientProvider struct {
	Client client.Client
	mu     sync.Mutex
}

func (cp *ClientProvider) Lock() {
	cp.mu.Lock()
}

func (cp *ClientProvider) Unlock() {
	cp.mu.Unlock()
}
