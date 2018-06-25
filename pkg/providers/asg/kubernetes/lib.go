package kubernetes

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers/asg"
)

func init() {
	asg.Register("kubernetes", &kubernetes{})
}

type EtcdInstance struct {
	N string `json:"name"`
	A string `json:"address"`
}

func (i EtcdInstance) Name() string {
	return i.N
}

func (i EtcdInstance) Address() string {
	return i.A
}

type EtcdCluster struct {
	Spec struct {
		Instances []EtcdInstance
		Size      int
	} `json:"spec"`
}

type kubernetes struct {
	config Config
}

type Config struct {
	MembershipFile string `json:"membership-file"`
}

func (k *kubernetes) Configure(providerConfig asg.Config) error {
	k.config = Config{}
	if err := providers.ParseParams(providerConfig.Params, &k.config); err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}
	return nil
}

func (k *kubernetes) Members() (instances []asg.Instance, self asg.Instance, size int, err error) {
	data, err := ioutil.ReadFile(k.config.MembershipFile)
	if err != nil {
		return nil, nil, 0, err
	}

	var cluster EtcdCluster
	err = yaml.Unmarshal(data, &cluster)
	if err != nil {
		return nil, nil, 0, err
	}

	hostname, _ := os.Hostname()

	instances = make([]asg.Instance, 0, len(cluster.Spec.Instances))
	for i := range cluster.Spec.Instances {
		instances = append(instances, &cluster.Spec.Instances[i])
		if strings.Contains(cluster.Spec.Instances[i].N, hostname) {
			self = &cluster.Spec.Instances[i]
		}
	}
	size = cluster.Spec.Size
	return
}
