//go:build corrosion_integration && corrosion_admin_integration

package corrosiontest_test

import (
	"context"
	"testing"

	"github.com/iris-xyz/go-corrosion"
	"github.com/iris-xyz/go-corrosion/corrosiontest"
)

// AdminClient can connect to the bind-mounted admin socket and query cluster
// membership. A single-node agent has no peers, so the call must succeed and
// return an empty slice (SWIM state tracks peers, not self).
//
// Gated behind the corrosion_admin_integration tag because Docker Desktop on
// macOS surfaces the bind-mounted socket inode but does not forward connections
// into the Linux VM — run this on a native-Linux host (CI, Hetzner, etc).
func TestAdminClient_ClusterMembershipStates(t *testing.T) {
	ctx := context.Background()

	agent, err := corrosiontest.StartAgent(ctx, corrosiontest.WithAdminSocketBind(t.TempDir()))
	if err != nil {
		t.Fatalf("StartAgent: %v", err)
	}
	t.Cleanup(func() { agent.Stop(context.Background()) })

	admin, err := corrosion.NewAdminClient(agent.AdminSocket())
	if err != nil {
		t.Fatalf("NewAdminClient: %v", err)
	}

	states, err := admin.ClusterMembershipStates(true)
	if err != nil {
		t.Fatalf("ClusterMembershipStates: %v", err)
	}
	if len(states) != 0 {
		t.Errorf("want 0 peers on single-node agent, got %d: %+v", len(states), states)
	}
}
