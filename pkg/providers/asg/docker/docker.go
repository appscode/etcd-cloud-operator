// Copyright 2017 Quentin Machu & eco authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docker

import (
	"fmt"
	"os"
	"strings"

	"github.com/kubedb/etcd-cloud-operator/pkg/providers"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers/asg"
)

func init() {
	asg.Register("docker", &docker{})
}

type docker struct {
	config config
}

type config struct {
	Size       int    `json:"size"`
	NameFilter string `json:"name-filter"`
}

func (d *docker) Configure(providerConfig asg.Config) error {
	d.config = config{Size: 3, NameFilter: "eco-"}
	if err := providers.ParseParams(providerConfig.Params, &d.config); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}
	return nil
}

func (d *docker) AutoScalingGroupStatus() (instances []asg.Instance, self asg.Instance, size int, err error) {
	hostname, _ := os.Hostname()

	// List all containers names, which match the filter.
	containerNames, err := containerList(d.config.NameFilter)
	if err != nil {
		return nil, nil, 0, err
	}

	for _, name := range containerNames {
		container, err := containerInspect(name)
		if err != nil {
			return nil, nil, 0, err
		}
		if strings.Contains(container.id, hostname) {
			self = container
		}
		instances = append(instances, container)
	}
	size = d.config.Size

	return
}
