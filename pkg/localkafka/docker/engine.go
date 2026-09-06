package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"

	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/recipe"
)

// EngineDriver talks to a real Docker Engine via the Go SDK.
type EngineDriver struct {
	cli *client.Client
}

var _ Driver = (*EngineDriver)(nil)

// NewEngineDriver connects to the Engine. dockerHost overrides DOCKER_HOST when
// non-empty; otherwise the SDK's env / platform default is used.
func NewEngineDriver(dockerHost string) (*EngineDriver, error) {
	opts := []client.Opt{client.FromEnv, client.WithAPIVersionNegotiation()}
	if dockerHost != "" {
		opts = append(opts, client.WithHost(dockerHost))
	}
	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker: connect: %w", err)
	}
	return &EngineDriver{cli: cli}, nil
}

// Ping verifies the Engine is reachable.
func (d *EngineDriver) Ping(ctx context.Context) error {
	_, err := d.cli.Ping(ctx)
	return err
}

// Close releases the client.
func (d *EngineDriver) Close() error { return d.cli.Close() }

func (d *EngineDriver) List(ctx context.Context, agentName string) ([]Container, error) {
	items, err := d.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", recipe.LabelManagedBy+"="+agentName)),
	})
	if err != nil {
		return nil, fmt.Errorf("docker: list: %w", err)
	}
	out := make([]Container, 0, len(items))
	for _, it := range items {
		name := ""
		if len(it.Names) > 0 {
			name = it.Names[0][1:] // strip leading '/'
		}
		out = append(out, Container{
			ID:         it.ID,
			Name:       name,
			ClusterFRN: it.Labels[recipe.LabelCluster],
			RecipeHash: it.Labels[recipe.LabelRecipeHash],
			Running:    it.State == container.StateRunning,
		})
	}
	return out, nil
}

func (d *EngineDriver) EnsureImage(ctx context.Context, ref string) error {
	list, err := d.cli.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", ref)),
	})
	if err != nil {
		return fmt.Errorf("docker: image list: %w", err)
	}
	if len(list) > 0 {
		return nil
	}
	rc, err := d.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("docker: pull %s: %w", ref, err)
	}
	defer rc.Close()
	_, err = io.Copy(io.Discard, rc) // drain to completion
	return err
}

func (d *EngineDriver) Create(ctx context.Context, spec recipe.Spec) (string, error) {
	port := nat.Port(strconv.Itoa(9092) + "/tcp")
	cfg := &container.Config{
		Image:        spec.Image,
		Env:          spec.Env,
		Labels:       spec.Labels,
		ExposedPorts: nat.PortSet{port: struct{}{}},
	}
	hostCfg := &container.HostConfig{
		PortBindings: nat.PortMap{
			port: []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: strconv.Itoa(spec.HostPort)}},
		},
		Mounts: []mount.Mount{{
			Type:   mount.TypeVolume,
			Source: spec.VolumeName,
			Target: "/var/lib/kafka/data",
		}},
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
	}
	res, err := d.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, spec.ContainerName)
	if err != nil {
		return "", fmt.Errorf("docker: create %s: %w", spec.ContainerName, err)
	}
	return res.ID, nil
}

func (d *EngineDriver) Start(ctx context.Context, id string) error {
	if err := d.cli.ContainerStart(ctx, id, container.StartOptions{}); err != nil && !isNotModified(err) {
		return fmt.Errorf("docker: start: %w", err)
	}
	return nil
}

func (d *EngineDriver) Stop(ctx context.Context, id string) error {
	if err := d.cli.ContainerStop(ctx, id, container.StopOptions{}); err != nil &&
		!errdefs.IsNotFound(err) && !isNotModified(err) {
		return fmt.Errorf("docker: stop: %w", err)
	}
	return nil
}

func (d *EngineDriver) Remove(ctx context.Context, id, volumeName string, removeVolume bool) error {
	if err := d.cli.ContainerRemove(ctx, id, container.RemoveOptions{Force: true}); err != nil &&
		!errdefs.IsNotFound(err) {
		return fmt.Errorf("docker: remove container: %w", err)
	}
	if removeVolume && volumeName != "" {
		if err := d.cli.VolumeRemove(ctx, volumeName, true); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("docker: remove volume %s: %w", volumeName, err)
		}
	}
	return nil
}

// isNotModified matches the Engine's 304 for start/stop of a container already
// in the target state.
func isNotModified(err error) bool {
	var e interface{ NotModified() bool }
	return errors.As(err, &e) && e.NotModified()
}
