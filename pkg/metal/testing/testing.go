// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package testing

import "net"

var (
	SampleProviderSpec = map[string]interface{}{
		"labels": map[string]string{
			"shoot-name":      "my-shoot",
			"shoot-namespace": "my-shoot-namespace",
		},
		"machineClassName": "foo",
		"machinePoolName":  "foo",
		"serverLabels": map[string]string{
			"instance-type": "bar",
		},
		"ignitionSecret": map[string]string{
			"name": "foo",
		},
		"metaData": map[string]any{
			"foo": "bar",
			"baz": "100",
		},
		"image":             "my-image",
		"ignitionSecretKey": "ignition",
		"ignition": `passwd:
  users:
    - groups: [group1]
      name: xyz
      sshAuthorizedKeys: ssh-ed25519 AAABC3NzaC1lZDI1NTE5AAAAIGqrmrq1XwWnPJoSsAeuVcDQNqA5XQK
      shell: /bin/bash`,
		"dnsServers": []net.IP{
			net.ParseIP("1.2.3.4"),
			net.ParseIP("5.6.7.8"),
		},
	}

	SampleIgnition = map[string]interface{}{
		"ignition": map[string]interface{}{
			"version": "3.2.0",
		},
		"passwd": map[string]interface{}{
			"users": []interface{}{
				map[string]interface{}{
					"groups": []interface{}{"group1"},
					"name":   "xyz",
					"shell":  "/bin/bash",
				},
			},
		},
		"storage": map[string]interface{}{
			"files": []interface{}{
				map[string]interface{}{
					"overwrite": true,
					"path":      "/etc/hostname",
					"contents": map[string]interface{}{
						"compression": "",
						"source":      "data:,machine-0%0A",
					},
					"mode": 420.0,
				},
				map[string]interface{}{
					"overwrite": true,
					"path":      "/var/lib/metal-cloud-config/init.sh",
					"contents": map[string]interface{}{
						"source":      "data:,abcd%0A",
						"compression": "",
					},
					"mode": 493.0,
				},
				map[string]interface{}{
					"path": "/etc/systemd/resolved.conf.d/dns.conf",
					"contents": map[string]interface{}{
						"compression": "",
						"source":      "data:,%5BResolve%5D%0ADNS%3D1.2.3.4%0ADNS%3D5.6.7.8",
					},
					"mode": 420.0,
				},
				map[string]interface{}{
					"path": "/var/lib/metal-cloud-config/metadata",
					"contents": map[string]interface{}{
						"compression": "",
						"source":      "data:;base64,eyJiYXoiOiIxMDAiLCJmb28iOiJiYXIifQ==",
					},
					"mode": 420.0,
				},
			},
		},
		"systemd": map[string]interface{}{
			"units": []interface{}{
				map[string]interface{}{
					"contents": `[Unit]
Wants=network-online.target
After=network-online.target
ConditionPathExists=!/var/lib/metal-cloud-config/init.done

[Service]
Type=oneshot
ExecStart=/var/lib/metal-cloud-config/init.sh
ExecStopPost=touch /var/lib/metal-cloud-config/init.done
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
`,
					"enabled": true,
					"name":    "cloud-config-init.service",
				},
			},
		},
	}

	SampleIgnitionWithFooHostname = map[string]interface{}{
		"ignition": map[string]interface{}{
			"version": "3.2.0",
		},
		"passwd": map[string]interface{}{
			"users": []interface{}{
				map[string]interface{}{
					"groups": []interface{}{"group1"},
					"name":   "xyz",
					"shell":  "/bin/bash",
				},
			},
		},
		"storage": map[string]interface{}{
			"files": []interface{}{
				map[string]interface{}{
					"overwrite": true,
					"path":      "/etc/hostname",
					"contents": map[string]interface{}{
						"compression": "",
						"source":      "data:,foo%0A",
					},
					"mode": 420.0,
				},
				map[string]interface{}{
					"overwrite": true,
					"path":      "/var/lib/metal-cloud-config/init.sh",
					"contents": map[string]interface{}{
						"source":      "data:,abcd%0A",
						"compression": "",
					},
					"mode": 493.0,
				},
				map[string]interface{}{
					"path": "/etc/systemd/resolved.conf.d/dns.conf",
					"contents": map[string]interface{}{
						"compression": "",
						"source":      "data:,%5BResolve%5D%0ADNS%3D1.2.3.4%0ADNS%3D5.6.7.8",
					},
					"mode": 420.0,
				},
				map[string]interface{}{
					"path": "/var/lib/metal-cloud-config/metadata",
					"contents": map[string]interface{}{
						"compression": "",
						"source":      "data:;base64,eyJiYXoiOiIxMDAiLCJmb28iOiJiYXIifQ==",
					},
					"mode": 420.0,
				},
			},
		},
		"systemd": map[string]interface{}{
			"units": []interface{}{
				map[string]interface{}{
					"contents": `[Unit]
Wants=network-online.target
After=network-online.target
ConditionPathExists=!/var/lib/metal-cloud-config/init.done

[Service]
Type=oneshot
ExecStart=/var/lib/metal-cloud-config/init.sh
ExecStopPost=touch /var/lib/metal-cloud-config/init.done
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
`,
					"enabled": true,
					"name":    "cloud-config-init.service",
				},
			},
		},
	}
)
