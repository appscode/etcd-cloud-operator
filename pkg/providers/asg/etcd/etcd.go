package etcd

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/ghodss/yaml"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers/asg"
)

func init() {
	asg.Register("etcd", &etcd{})
}

type instance struct {
	name, address string
}

func (i *instance) Name() string {
	return i.name
}

func (i *instance) Address() string {
	return i.address
}

type Cluster struct {
	Instances map[string]string
	Size      int
}

type etcd struct {
	config config
	cache  map[string]string
	lock   sync.RWMutex
}

type config struct {
	Name           string `json:"name"`
	Size           int    `json:"size"`
	InitialCluster string `json:"initial-cluster"`

	CacheFile string `json:"cache-file"`
}

func (d *etcd) Configure(providerconfig asg.Config) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.config = config{}
	if err := providers.ParseParams(providerconfig.Params, &d.config); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	m, err := providers.ParseCluster(d.config.InitialCluster)
	if err != nil {
		return err
	}
	if len(m) != d.config.Size {
		return fmt.Errorf("expected initial-cluster %s to have length %d, found %d", d.config.InitialCluster, d.config.Size, len(m))
	}

	d.cache = make(map[string]string)
	for k, v := range m {
		d.cache[k] = v
	}

	// reaload from caches membership file, in case machine rebooted
	if data, err := ioutil.ReadFile(d.config.CacheFile); err == nil {
		var cluster Cluster
		if err = yaml.Unmarshal(data, &cluster); err == nil {
			for k, v := range cluster.Instances {
				d.cache[k] = v
			}
		}
	}

	return nil
}

func (d *etcd) AutoScalingGroupStatus() (instances []asg.Instance, self asg.Instance, size int, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	instances = make([]asg.Instance, 0, d.config.Size)
	for k, v := range d.cache {
		i := &instance{
			name:    k,
			address: v,
		}
		if i.name == d.config.Name {
			self = i
		}
		instances = append(instances, i)
	}
	size = d.config.Size
	return
}
