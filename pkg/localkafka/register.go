package localkafka

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
	"github.com/KafkaMetamorphosis/franz/pkg/localkafka/recipe"
)

// provisioningSchema is the advisory franz.provisioning/* schema this agent
// advertises (003.9, ADR-API-008) so the console can pre-fill and constrain a
// Kafka Cluster form that targets it. The agent reads exactly these keys.
func provisioningSchema(defaultVersion string) []*franzv1.ProvisioningLabelSpec {
	sp := func(key, desc, def string, allowed []string, required bool) *franzv1.ProvisioningLabelSpec {
		b := franzv1.ProvisioningLabelSpec_builder{
			Key: &key, Description: &desc, DefaultValue: &def, Required: &required,
		}
		if len(allowed) > 0 {
			b.AllowedValues = allowed
		}
		return b.Build()
	}
	return []*franzv1.ProvisioningLabelSpec{
		sp(recipe.DeploymentTypeLabel, "Selects the recipe family.", recipe.LocalDocker,
			[]string{recipe.LocalDocker}, true),
		sp(recipe.KafkaVersionLabel, "apache/kafka image tag when kafka-image is unset.",
			defaultVersion, nil, false),
		sp(recipe.KafkaImageLabel, "Full apache/kafka-compatible image ref (tag, digest, or mirror). Overrides kafka-version.",
			"", nil, false),
	}
}

// EnsureRegistered is a local-dev convenience (FRANZ_REGISTER=1): it makes sure
// an agent named `name` exists in Franz and returns a usable bearer token.
//
//   - agent missing  → CreateAgent, return the fresh token
//   - agent present  → RotateAgentToken, return the new token (the registration
//     is reused; only the token is refreshed — any previously-issued token is
//     invalidated, which is fine for a single local agent)
//   - agent deleted  → error (the name is reserved; pick another FRANZ_AGENT_NAME)
//
// AgentService is not behind the agent-auth interceptor (it is a console API), so
// this runs on an unauthenticated connection. provisioningLabels is the agent's
// advisory schema — set on create and refreshed on the reuse path.
func EnsureRegistered(
	ctx context.Context, conn *grpc.ClientConn, name string,
	provisioningLabels []*franzv1.ProvisioningLabelSpec,
) (token string, created bool, err error) {
	ac := franzv1.NewAgentServiceClient(conn)

	got, getErr := ac.GetAgent(ctx, franzv1.GetAgentRequest_builder{Name: &name}.Build())
	switch {
	case status.Code(getErr) == codes.NotFound:
		t := franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER
		resp, cErr := ac.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
			Name:               &name,
			Type:               &t,
			Labels:             map[string]string{"franz.role": "local-kafka-agent"},
			ProvisioningLabels: provisioningLabels,
		}.Build())
		if cErr != nil {
			return "", false, fmt.Errorf("register agent %q: %w", name, cErr)
		}
		return resp.GetToken(), true, nil

	case getErr != nil:
		return "", false, fmt.Errorf("look up agent %q: %w", name, getErr)
	}

	if got.GetAgent().GetStatus() == franzv1.AgentStatus_AGENT_STATUS_DELETED {
		return "", false, fmt.Errorf("agent %q is deleted in Franz and its name is reserved — set FRANZ_AGENT_NAME to a new name", name)
	}

	// Refresh the advertised schema in case this agent version changed its keys.
	if len(provisioningLabels) > 0 {
		mask := "provisioning_labels"
		if _, uErr := ac.UpdateAgent(ctx, franzv1.UpdateAgentRequest_builder{
			Name:               &name,
			ProvisioningLabels: provisioningLabels,
			UpdateMask:         &fieldmaskpb.FieldMask{Paths: []string{mask}},
		}.Build()); uErr != nil {
			return "", false, fmt.Errorf("refresh schema for agent %q: %w", name, uErr)
		}
	}

	resp, rErr := ac.RotateAgentToken(ctx, franzv1.RotateAgentTokenRequest_builder{Name: &name}.Build())
	if rErr != nil {
		return "", false, fmt.Errorf("rotate token for agent %q: %w", name, rErr)
	}
	return resp.GetToken(), false, nil
}
