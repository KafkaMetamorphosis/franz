package models

import (
	"errors"
	"log"
	"time"
)

type Metadata struct {
	Name   string
	Labels map[string]string
}

type AdvertiseListener struct {
	Name string
	Host string
	Port int
	Tls  bool
}

type ClusterClaimSpec struct {
	AdvertiseListeners []AdvertiseListener
	Version            string
	Configs            map[string]string
}

type ClusterClaim struct {
	Metadata  Metadata
	Spec      ClusterClaimSpec
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (cc *ClusterClaim) Validate() error {
	if cc.Metadata.Name == "" {
		return errors.New("cluster name is required")
	}

	if len(cc.Spec.AdvertiseListeners) == 0 {
		log.Printf("b %v", cc)
		return errors.New("at least one advertise listener should be provided")
	}

	return nil
}
