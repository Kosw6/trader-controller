package postgres

import "testing"

func TestJobsEventsTopicIsolation(t *testing.T) {
	for _, tc := range []struct{ configured, want string }{
		{"", "trader.jobs.events"},
		{"   ", "trader.jobs.events"},
		{" trader.jobs.events649 ", "trader.jobs.events649"},
	} {
		t.Run(tc.configured, func(t *testing.T) {
			t.Setenv("KAFKA_TOPIC", tc.configured)
			if got := jobsEventsTopic(); got != tc.want {
				t.Fatalf("outbox enqueue topic = %q, want %q", got, tc.want)
			}
		})
	}
}
