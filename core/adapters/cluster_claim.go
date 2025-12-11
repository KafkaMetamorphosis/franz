package adapters

import (
	"github.com/franz-kafka/server/core/models"
	"github.com/franz-kafka/server/core/wire/in"
	"github.com/franz-kafka/server/core/wire/out"
)

func ClusterClaimAdvertiseListernerToWire(advertiseListener models.AdvertiseListener) out.AdvertiseListener {
	return out.AdvertiseListener{
		Name: advertiseListener.Name,
		Host: advertiseListener.Host,
		Port: advertiseListener.Port,
		Tls:  advertiseListener.Tls,
	}
}

func ClusterClaimToWire(clusterClaimModel *models.ClusterClaim) out.ClusterClaim {
	advertiseListenersWire := make([]out.AdvertiseListener, 0)
	for _, advertiseListener := range clusterClaimModel.Spec.AdvertiseListeners {
		advertiseListenersWire = append(advertiseListenersWire, ClusterClaimAdvertiseListernerToWire(advertiseListener))
	}
	return out.ClusterClaim{
		Metadata: out.Metadata{
			Name:   clusterClaimModel.Metadata.Name,
			Labels: clusterClaimModel.Metadata.Labels,
		},
		Spec: out.ClusterClaimSpec{
			AdvertiseListeners: advertiseListenersWire,
			Version:            clusterClaimModel.Spec.Version,
			Configs:            clusterClaimModel.Spec.Configs,
		},
		CreatedAt: clusterClaimModel.CreatedAt,
		UpdatedAt: clusterClaimModel.UpdatedAt,
	}
}

func ClusterClaimsToWire(clusterClaimsModels []*models.ClusterClaim) []out.ClusterClaim {
	clusterClaimsWire := make([]out.ClusterClaim, 0)
	for _, clusterClaim := range clusterClaimsModels {
		clusterClaimsWire = append(clusterClaimsWire, ClusterClaimToWire(clusterClaim))
	}

	return clusterClaimsWire
}

func ClusterClaimAdvertiseListernerFromWire(advertiseListener in.AdvertiseListener) models.AdvertiseListener {
	return models.AdvertiseListener{
		Name: advertiseListener.Name,
		Host: advertiseListener.Host,
		Port: advertiseListener.Port,
		Tls:  advertiseListener.Tls,
	}
}

func ClusterClaimFromWire(clusterClainWire in.ClusterClaim) *models.ClusterClaim {
	advertiseListeners := make([]models.AdvertiseListener, 0)
	for _, advertiseListener := range clusterClainWire.Spec.AdvertiseListeners {
		advertiseListeners = append(advertiseListeners, ClusterClaimAdvertiseListernerFromWire(advertiseListener))
	}

	return &models.ClusterClaim{
		Metadata: models.Metadata{
			Name:   clusterClainWire.Metadata.Name,
			Labels: clusterClainWire.Metadata.Labels,
		},
		Spec: models.ClusterClaimSpec{
			Version:            clusterClainWire.Spec.Version,
			Configs:            clusterClainWire.Spec.Configs,
			AdvertiseListeners: advertiseListeners,
		},
	}
}
