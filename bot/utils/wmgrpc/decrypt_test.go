package wmgrpc

import (
	"testing"

	"github.com/grafov/m3u8"
)

func TestResolveSegmentKeyURIsInheritsActiveKey(t *testing.T) {
	contentKey := "skd://itunes.apple.com/content/key"
	segments := []*m3u8.MediaSegment{
		{Key: &m3u8.Key{URI: prefetchKey}},
		{Key: &m3u8.Key{URI: contentKey}},
		{},
		nil,
		{},
	}

	got := resolveSegmentKeyURIs(segments)
	want := []string{
		prefetchKey,
		contentKey,
		contentKey,
		"",
		contentKey,
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("segment %d key = %q, want %q", i, got[i], want[i])
		}
	}
}
