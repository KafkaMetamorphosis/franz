package out

import "time"

type Metadata struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
}

type AdvertiseListener struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
	Tls  bool   `json:"tls,omitempty"`
}

type ClusterClaimSpec struct {
	AdvertiseListeners []AdvertiseListener `json:"advertise_listeners"`
	Version            string              `json:"version"`
	Configs            map[string]string   `json:"configs"`
}

type BrokerState struct {
	Id     string
	RackId string
}

type ClusterState struct {
	Brokers         []BrokerState
	BrokersQuantity int
}

type ClusterClaim struct {
	Metadata  Metadata         `json:"metadata"`
	Spec      ClusterClaimSpec `json:"spec"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	State     ClusterState     `json:"state,omitempty"`
}
