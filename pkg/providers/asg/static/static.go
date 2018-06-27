package static

import (
	"fmt"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers/asg"
)

func init() {
	asg.Register("static", &static{})
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

type static struct {
	config    config
	self      asg.Instance
	instances []asg.Instance
}

type config struct {
	Name           string `json:"name"`
	Size           int    `json:"size"`
	InitialCluster string `json:"initial-cluster"`
}

func (d *static) Configure(providerConfig asg.Config) error {
	d.config = config{}
	if err := providers.ParseParams(providerConfig.Params, &d.config); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}

	m, err := providers.ParseCluster(d.config.InitialCluster)
	if err != nil {
		return err
	}
	if len(m) != d.config.Size {
		return fmt.Errorf("expected initial-cluster %s to have length %d, found %d", d.config.InitialCluster, d.config.Size, len(m))
	}

	d.instances = make([]asg.Instance, 0, len(m))
	for k, v := range m {
		i := &instance{
			name:    k,
			address: v,
		}
		if i.name == d.config.Name {
			d.self = i
		}
		d.instances = append(d.instances, i)
	}
	return nil
}

func (d *static) AutoScalingGroupStatus() (instances []asg.Instance, self asg.Instance, size int, err error) {
	return d.instances, d.self, len(d.instances), nil
}

func (d *static) UID() string {
	return "static"
}

func (d *static) Refresh(map[string]string) error {
	return nil
}
