package worker

import "testing"

func TestSharedHostingExcludesCapacityChangersButKeepsCoordination(t *testing.T) {
	definitions := []Definition{{Name: "outbox-relay"}, {Name: "kafka-health"}, {Name: "consumer-lag-monitor"},
		{Name: "job-status-aggregator"}, {Name: "worker-scale-planner"}, {Name: "local-docker-actuator"}, {Name: "aws-asg-actuator"}}
	got := ForHostingMode(definitions, true)
	if len(got) != 4 {
		t.Fatalf("shared definitions=%d", len(got))
	}
	for _, d := range got {
		switch d.Name {
		case "worker-scale-planner", "local-docker-actuator", "aws-asg-actuator":
			t.Fatal(d.Name)
		}
	}
	if len(ForHostingMode(definitions, false)) != len(definitions) {
		t.Fatal("dedicated behavior changed")
	}
}
