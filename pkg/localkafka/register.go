package localkafka

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

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
// this runs on an unauthenticated connection.
func EnsureRegistered(ctx context.Context, conn *grpc.ClientConn, name string) (token string, created bool, err error) {
	ac := franzv1.NewAgentServiceClient(conn)

	got, getErr := ac.GetAgent(ctx, franzv1.GetAgentRequest_builder{Name: &name}.Build())
	switch {
	case status.Code(getErr) == codes.NotFound:
		t := franzv1.AgentType_AGENT_TYPE_CLUSTER_PROVIDER
		resp, cErr := ac.CreateAgent(ctx, franzv1.CreateAgentRequest_builder{
			Name:   &name,
			Type:   &t,
			Labels: map[string]string{"franz.role": "local-kafka-agent"},
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

	resp, rErr := ac.RotateAgentToken(ctx, franzv1.RotateAgentTokenRequest_builder{Name: &name}.Build())
	if rErr != nil {
		return "", false, fmt.Errorf("rotate token for agent %q: %w", name, rErr)
	}
	return resp.GetToken(), false, nil
}
