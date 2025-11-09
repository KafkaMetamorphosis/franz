package models

import "errors"

// Cluster errors
var (
	ErrClusterNameRequired         = errors.New("cluster name is required")
	ErrClusterBootstrapURLRequired = errors.New("cluster bootstrap URL is required")
	ErrClusterNotFound             = errors.New("cluster not found")
)

// Topic definition errors
var (
	ErrTopicDefinitionNameRequired = errors.New("topic definition name is required")
	ErrTopicDefinitionNotFound     = errors.New("topic definition not found")
)

// Cluster topic errors
var (
	ErrClusterTopicNameRequired    = errors.New("topic name is required")
	ErrClusterTopicClusterRequired = errors.New("cluster name is required")
	ErrClusterTopicNotFound        = errors.New("cluster topic not found")
)
