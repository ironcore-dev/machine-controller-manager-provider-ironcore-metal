// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package testing

import (
	"net"
)

var (
	SampleProviderSpec = map[string]any{
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

	SampleIgnition = map[string]any{
		"ignition": map[string]any{
			"version": "3.2.0",
		},
		"passwd": map[string]any{
			"users": []any{
				map[string]any{
					"groups": []any{"group1"},
					"name":   "xyz",
					"shell":  "/bin/bash",
				},
			},
		},
		"storage": map[string]any{
			"files": []any{
				map[string]any{
					"overwrite": true,
					"path":      "/etc/hostname",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:,machine-0%0A",
					},
					"mode": 420.0,
				},
				map[string]any{
					"overwrite": true,
					"path":      "/var/lib/metal-cloud-config/init.sh",
					"contents": map[string]any{
						"source":      "data:,abcd%0A",
						"compression": "",
					},
					"mode": 493.0,
				},
				map[string]any{
					"path": "/etc/systemd/resolved.conf.d/dns.conf",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:,%5BResolve%5D%0ADNS%3D1.2.3.4%0ADNS%3D5.6.7.8",
					},
					"mode": 420.0,
				},
				map[string]any{
					"path": "/var/lib/metal-cloud-config/metadata",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:;base64,eyJiYXoiOiIxMDAiLCJmb28iOiJiYXIifQ==",
					},
					"mode": 420.0,
				},
			},
		},
		"systemd": map[string]any{
			"units": []any{
				map[string]any{
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

	SampleIgnitionWithServerMetadata = map[string]any{
		"ignition": map[string]any{
			"version": "3.2.0",
		},
		"passwd": map[string]any{
			"users": []any{
				map[string]any{
					"groups": []any{"group1"},
					"name":   "xyz",
					"shell":  "/bin/bash",
				},
			},
		},
		"storage": map[string]any{
			"files": []any{
				map[string]any{
					"overwrite": true,
					"path":      "/etc/hostname",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:,machine-0%0A",
					},
					"mode": 420.0,
				},
				map[string]any{
					"overwrite": true,
					"path":      "/var/lib/metal-cloud-config/init.sh",
					"contents": map[string]any{
						"source":      "data:,abcd%0A",
						"compression": "",
					},
					"mode": 493.0,
				},
				map[string]any{
					"path": "/etc/systemd/resolved.conf.d/dns.conf",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:,%5BResolve%5D%0ADNS%3D1.2.3.4%0ADNS%3D5.6.7.8",
					},
					"mode": 420.0,
				},
				map[string]any{
					"path": "/var/lib/metal-cloud-config/metadata",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:;base64,eyJiYXoiOiIxMDAiLCJmb28iOiJiYXIiLCJsb29wYmFja0FkZHJlc3MiOiIyMDAxOmRiODo6MSJ9",
					},
					"mode": 420.0,
				},
			},
		},
		"systemd": map[string]any{
			"units": []any{
				map[string]any{
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

	SampleIgnitionWithTestServerHostname = map[string]any{
		"ignition": map[string]any{
			"version": "3.2.0",
		},
		"passwd": map[string]any{
			"users": []any{
				map[string]any{
					"groups": []any{"group1"},
					"name":   "xyz",
					"shell":  "/bin/bash",
				},
			},
		},
		"storage": map[string]any{
			"files": []any{
				map[string]any{
					"overwrite": true,
					"path":      "/etc/hostname",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:,test-server%0A",
					},
					"mode": 420.0,
				},
				map[string]any{
					"overwrite": true,
					"path":      "/var/lib/metal-cloud-config/init.sh",
					"contents": map[string]any{
						"source":      "data:,abcd%0A",
						"compression": "",
					},
					"mode": 493.0,
				},
				map[string]any{
					"path": "/etc/systemd/resolved.conf.d/dns.conf",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:,%5BResolve%5D%0ADNS%3D1.2.3.4%0ADNS%3D5.6.7.8",
					},
					"mode": 420.0,
				},
				map[string]any{
					"path": "/var/lib/metal-cloud-config/metadata",
					"contents": map[string]any{
						"compression": "",
						"source":      "data:;base64,eyJiYXoiOiIxMDAiLCJmb28iOiJiYXIifQ==",
					},
					"mode": 420.0,
				},
			},
		},
		"systemd": map[string]any{
			"units": []any{
				map[string]any{
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
