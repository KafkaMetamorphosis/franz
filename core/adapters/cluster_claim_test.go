package adapters

import (
	"testing"

	"github.com/franz-kafka/server/core/models"
	"github.com/franz-kafka/server/core/wire/out"
)

func TestClusterClaimAdvertiseListernerToWire(t *testing.T) {
	tests := []struct {
		name     string
		input    models.AdvertiseListener
		expected out.AdvertiseListener
	}{
		{
			// Case 1: Happy Path - Complete AdvertiseListener
			name: "Complete AdvertiseListener with all fields",
			input: models.AdvertiseListener{
				Name: "PLAINTEXT",
				Host: "kafka.example.com",
				Port: 9092,
				Tls:  false,
			},
			expected: out.AdvertiseListener{
				Name: "PLAINTEXT",
				Host: "kafka.example.com",
				Port: 9092,
				Tls:  false,
			},
		},
		{
			// Case 2: Happy Path - TLS Enabled
			name: "TLS enabled AdvertiseListener",
			input: models.AdvertiseListener{
				Name: "SSL",
				Host: "kafka-ssl.example.com",
				Port: 9093,
				Tls:  true,
			},
			expected: out.AdvertiseListener{
				Name: "SSL",
				Host: "kafka-ssl.example.com",
				Port: 9093,
				Tls:  true,
			},
		},
		{
			// Case 7: Special Characters in Name
			name: "Special characters in name",
			input: models.AdvertiseListener{
				Name: "LISTENER-123_ABC",
				Host: "kafka.example.com",
				Port: 9092,
				Tls:  false,
			},
			expected: out.AdvertiseListener{
				Name: "LISTENER-123_ABC",
				Host: "kafka.example.com",
				Port: 9092,
				Tls:  false,
			},
		},
		{
			// Case 10: Localhost Variants
			name: "Localhost IP address",
			input: models.AdvertiseListener{
				Name: "LOCAL",
				Host: "127.0.0.1",
				Port: 9092,
				Tls:  false,
			},
			expected: out.AdvertiseListener{
				Name: "LOCAL",
				Host: "127.0.0.1",
				Port: 9092,
				Tls:  false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClusterClaimAdvertiseListernerToWire(tt.input)

			if result.Name != tt.expected.Name {
				t.Errorf("Name: got %q, want %q", result.Name, tt.expected.Name)
			}
			if result.Host != tt.expected.Host {
				t.Errorf("Host: got %q, want %q", result.Host, tt.expected.Host)
			}
			if result.Port != tt.expected.Port {
				t.Errorf("Port: got %d, want %d", result.Port, tt.expected.Port)
			}
			if result.Tls != tt.expected.Tls {
				t.Errorf("Tls: got %v, want %v", result.Tls, tt.expected.Tls)
			}
		})
	}
}
