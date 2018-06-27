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

package providers

import (
	"reflect"
	"unsafe"

	"github.com/ghodss/yaml"
	"strings"
	"fmt"
	"net/url"
)

func ParseParams(params map[string]interface{}, cfg interface{}) error {
	yConfig, err := yaml.Marshal(params)
	if err != nil {
		return err
	}

	obj := reflect.NewAt(reflect.TypeOf(cfg).Elem(), unsafe.Pointer(reflect.ValueOf(cfg).Pointer())).Interface()
	if err := yaml.Unmarshal(yConfig, obj); err != nil {
		return err
	}

	return nil
}

func ParseCluster(initialCluster string) (map[string]string, error) {
	parts := strings.FieldsFunc(initialCluster, func(r rune) bool { return r == ',' || r == '=' })
	n := len(parts)
	if n == 0 || n%2 != 0 {
		return nil, fmt.Errorf("invalid input %v", initialCluster)
	}

	instances := make(map[string]string, n/2+1)
	for i := 0; i < n; i += 2 {
		name := strings.TrimSpace(parts[i])
		u, err := url.Parse(parts[i+1])
		if err != nil {
			return nil, fmt.Errorf("invalid url %s for instance %s", parts[i+1], name)
		}
		if _, ok := instances[name]; ok {
			return nil, fmt.Errorf("instance name %s is found multiple times", name)
		}
		instances[name] = u.Hostname()
	}
	return instances, nil
}
