package recoverycoord

import "testing"

func TestRecoveryTopicFence(t *testing.T) {
	if New(nil).rawTopic != DefaultRawTopic {
		t.Fatal("default changed")
	}
	for _, topic := range []string{DefaultRawTopic, DefaultRawTopic + "649"} {
		c, err := NewWithRawTopic(nil, topic)
		if err != nil || c.rawTopic != topic {
			t.Fatal("approved topic rejected")
		}
	}
	for _, topic := range []string{"", "trader.jobs.events", "trader.raw.news.ready649", DefaultRawTopic + "/bad", DefaultRawTopic + "650"} {
		if _, err := NewWithRawTopic(nil, topic); err != ErrInvalid {
			t.Fatal("unapproved topic accepted")
		}
	}
}
