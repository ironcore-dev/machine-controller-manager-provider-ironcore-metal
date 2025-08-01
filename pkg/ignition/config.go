// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package ignition

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	buconfig "github.com/coreos/butane/config"
	"github.com/coreos/butane/config/common"
	"github.com/imdario/mergo"
	"sigs.k8s.io/yaml"
)

var (
	//go:embed ignition.tmpl
	IgnitionTemplate string
)

const (
	dnsConfFile    = "/etc/systemd/resolved.conf.d/dns.conf"
	dnsEqualString = "DNS="
	metaDataFile   = "/var/lib/metal-cloud-config/metadata"
	fileMode       = 0644
)

type Config struct {
	Hostname         string
	UserData         string
	MetaData         map[string]any
	Ignition         string
	IgnitionOverride bool
	DnsServers       []netip.Addr
}

func Render(config *Config) (string, error) {
	ignitionBase := &map[string]any{}
	if err := yaml.Unmarshal([]byte(IgnitionTemplate), ignitionBase); err != nil {
		return "", err
	}

	// if ignition was set in providerSpec merge it with our template
	if config.Ignition != "" {
		additional := map[string]any{}

		if err := yaml.Unmarshal([]byte(config.Ignition), &additional); err != nil {
			return "", err
		}

		// default to append ignition
		opt := mergo.WithAppendSlice

		// allow also to fully override
		if config.IgnitionOverride {
			opt = mergo.WithOverride
		}

		// merge both ignitions
		err := mergo.Merge(ignitionBase, additional, opt)
		if err != nil {
			return "", err
		}
	}

	if len(config.DnsServers) > 0 {
		dnsServers := []string{"[Resolve]"}
		for _, v := range config.DnsServers {
			dnsEntry := fmt.Sprintf("%s%s", dnsEqualString, v.String())
			dnsServers = append(dnsServers, dnsEntry)
		}

		dnsConf := map[string]any{
			"storage": map[string]any{
				"files": []any{map[string]any{
					"path": dnsConfFile,
					"mode": fileMode,
					"contents": map[string]any{
						"inline": strings.Join(dnsServers, "\n"),
					},
				}},
			},
		}

		// merge dnsConfiguration with ignition content
		if err := mergo.Merge(ignitionBase, dnsConf, mergo.WithAppendSlice); err != nil {
			return "", fmt.Errorf("failed to merge dnsServer configuration with igntition content: %w", err)
		}
	}

	if len(config.MetaData) > 0 {
		metaDataJSON, err := json.Marshal(config.MetaData)
		if err != nil {
			return "", fmt.Errorf("failed to marshal MetaData to JSON: %w", err)
		}

		metaDataConf := map[string]any{
			"storage": map[string]any{
				"files": []any{map[string]any{
					"path": metaDataFile,
					"mode": fileMode,
					"contents": map[string]any{
						"inline": string(metaDataJSON),
					},
				}},
			},
		}

		// merge metaData configuration with ignition content
		if err := mergo.Merge(ignitionBase, metaDataConf, mergo.WithAppendSlice); err != nil {
			return "", fmt.Errorf("failed to merge metaData configuration with ignition content: %w", err)
		}
	}

	mergedIgnition, err := yaml.Marshal(ignitionBase)
	if err != nil {
		return "", err
	}

	tmpl, err := template.New("ignition").Funcs(sprig.HermeticTxtFuncMap()).Parse(string(mergedIgnition))
	if err != nil {
		return "", fmt.Errorf("failed creating ignition file: %w", err)
	}

	buf := bytes.NewBufferString("")
	err = tmpl.Execute(buf, config)
	if err != nil {
		return "", fmt.Errorf("failed creating ignition file while executing template: %w", err)
	}

	ignition, err := renderButane(buf.Bytes())
	if err != nil {
		return "", err
	}

	return ignition, nil
}

func renderButane(dataIn []byte) (string, error) {
	// render by butane to json
	options := common.TranslateBytesOptions{
		Raw:    true,
		Pretty: false,
	}
	options.NoResourceAutoCompression = true
	dataOut, _, err := buconfig.TranslateBytes(dataIn, options)
	if err != nil {
		return "", err
	}
	return string(dataOut), nil
}
