package osm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"

	otx "github.com/appscode/osm/context"
	"github.com/graymeta/stow"
	"github.com/kubedb/etcd-cloud-operator/pkg/providers/snapshot"
	log "github.com/sirupsen/logrus"
)

func init() {
	snapshot.Register("s3", &s3{})
}

type s3 struct {
	config config
	region string
}

type config struct {
	ConfigFile string `json:"config-file"`
	Bucket     string `json:"bucket"`
}

func (s *s3) connect() (stow.Container, error) {
	cfg, err := otx.LoadConfig(s.config.ConfigFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load osm config: %v", err)
	}

	loc, err := cfg.Dial(cfg.CurrentContext)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %v", err)
	}

	c, err := loc.Container(s.config.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to find container: %v", err)
	}
	return c, nil
}

func (s *s3) Configure(providerConfig snapshot.Config) error {
	s.config.ConfigFile = providerConfig.ConfigFile
	s.config.Bucket = providerConfig.Bucket

	if s.config.Bucket == "" {
		return errors.New("invalid configuration: bucket name is missing")
	}

	if s.config.ConfigFile == "" {
		return errors.New("invalid configuration: config file is missing")
	}

	c, err := s.connect()
	if err != nil {
		return err
	}

	r := bytes.NewReader([]byte("CheckBucketAccess"))
	item, err := c.Put(".eco", r, r.Size(), nil)
	if err != nil {
		return err
	}
	if err := c.RemoveItem(item.ID()); err != nil {
		return err
	}
	return nil
}

func (s *s3) Save(r io.ReadCloser, metadata *snapshot.Metadata) error {
	key := metadata.Filename()

	c, err := s.connect()
	if err != nil {
		return err
	}

	_, err = c.Put(key, r, -1, nil)
	if err != nil {
		return fmt.Errorf("failed to upload object: %v", err)
	}

	item, err := c.Item(key)
	if err != nil {
		return err
	}

	metadata.Size, _ = item.Size()
	return nil
}

func (s *s3) Get(metadata *snapshot.Metadata) (string, bool, error) {
	c, err := s.connect()
	if err != nil {
		return "", false, err
	}

	item, err := c.Item(metadata.Name)
	if err != nil {
		return "", false, err
	}

	rd, err := item.Open()
	if err != nil {
		return "", false, err
	}
	defer rd.Close()

	f, err := ioutil.TempFile("", "")
	if err != nil {
		return "", true, err
	}

	if _, err = io.Copy(f, rd); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", true, fmt.Errorf("failed to get aws s3 object: %v", err)
	}

	f.Sync()
	f.Close()

	return f.Name(), true, nil
}

func (s *s3) Info() (*snapshot.Metadata, error) {
	c, err := s.connect()
	if err != nil {
		return nil, err
	}

	var metadatas []*snapshot.Metadata

	cursor := stow.CursorStart
	for {
		items, cursor, err := c.Items("", cursor, 50)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %v", err)
		}
		for _, item := range items {
			sz, err := item.Size()
			if err != nil {
				log.Warnf("failed to parse metadata for snapshot %v", item.ID())
				continue
			}

			metadata, err := snapshot.NewMetadata(item.Name(), -1, sz, s)
			if err != nil {
				log.Warnf("failed to parse metadata for snapshot %v", item.ID())
				continue
			}
			metadatas = append(metadatas, metadata)
		}
		if stow.IsCursorEnd(cursor) {
			break
		}
	}
	if len(metadatas) == 0 {
		return nil, snapshot.ErrNoSnapshot
	}
	sort.Sort(snapshot.MetadataSorter(metadatas))

	return metadatas[len(metadatas)-1], nil
}

func (s *s3) Purge(ttl time.Duration) error {
	c, err := s.connect()
	if err != nil {
		return err
	}

	cursor := stow.CursorStart
	for {
		items, cursor, err := c.Items("", cursor, 50)
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}
		for _, item := range items {
			mod, err := item.LastMod()
			if err != nil {
				log.Warnf("failed to detect last modified time for snapshot %v", item.ID())
				continue
			}
			if time.Since(mod) > ttl {
				log.Infof("purging snapshot file %q because it is that older than %v", item.ID(), ttl)

				err = c.RemoveItem(item.ID())
				if err != nil {
					log.WithError(err).Warnf("failed to remove object %v", item.ID())
				}
			}
		}
		if stow.IsCursorEnd(cursor) {
			break
		}
	}
	return nil
}
