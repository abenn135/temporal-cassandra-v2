package testcore

import (
	"testing"

	"go.temporal.io/server/common/testing/parallelsuite"
)

type TestEnvSuite struct {
	parallelsuite.Suite[*TestEnvSuite]
}

func TestTestEnvSuite(t *testing.T) {
	parallelsuite.Run(t, &TestEnvSuite{})
}

func (s *TestEnvSuite) TestDedicatedClusterUsageTracker_NoPanicWithoutExplicitRequest() {
	tracker := newDedicatedClusterUsageTracker(false)

	s.NoError(tracker.unusedError())
}

func (s *TestEnvSuite) TestDedicatedClusterUsageTracker_PanicsWhenUnused() {
	tracker := newDedicatedClusterUsageTracker(true)

	s.EqualError(
		tracker.unusedError(),
		`testcore.WithDedicatedCluster() was requested but no dedicated-cluster-only feature was used (firstUseReason="")`,
	)
}

func (s *TestEnvSuite) TestDedicatedClusterUsageTracker_NoPanicAfterUse() {
	tracker := newDedicatedClusterUsageTracker(true)
	tracker.markUsed("global hook")

	s.NoError(tracker.unusedError())
}
