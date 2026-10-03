package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCollectionDeliveryKeepsMixedTrackOrder(t *testing.T) {
	// Cached IDs can be older than the fresh upload between them; repeated
	// playlist entries and tracks from another dump must retain their positions.
	tracks := []collectionDeliveryTrack{
		{dumpID: 1, msgID: 10}, {dumpID: 1, msgID: 100},
		{dumpID: 1, msgID: 20}, {dumpID: 1, msgID: 20},
		{}, {dumpID: 2, msgID: 30},
	}
	var received []int
	n, err := deliverOrderedCollection(context.Background(), tracks, func(_ int64, ids []int) error {
		received = append(received, ids...)
		return nil
	}, func(i int) bool {
		received = append(received, -i)
		return true
	})
	if err != nil || n != 6 || !reflect.DeepEqual(received, []int{10, 100, 20, 20, -4, 30}) {
		t.Fatalf("delivered=%d, error=%v, order=%v", n, err, received)
	}
}

func TestCollectionDeliveryRetriesFailedBatchBeforeLaterTracks(t *testing.T) {
	tracks := []collectionDeliveryTrack{
		{dumpID: 1, msgID: 1}, {dumpID: 1, msgID: 2}, {dumpID: 2, msgID: 3},
	}
	var received []int
	n, err := deliverOrderedCollection(context.Background(), tracks, func(dumpID int64, ids []int) error {
		if dumpID == 1 {
			return errors.New("deleted cache message")
		}
		received = append(received, ids...)
		return nil
	}, func(i int) bool {
		received = append(received, tracks[i].msgID)
		return i != 1 // The unrecoverable track is reported as a partial delivery.
	})
	if err != nil || n != 2 || !reflect.DeepEqual(received, []int{1, 2, 3}) {
		t.Fatalf("delivered=%d, error=%v, order=%v", n, err, received)
	}
}

func TestCollectionDeliveryCancellationStopsFallbackAndNextBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracks := []collectionDeliveryTrack{
		{dumpID: 1, msgID: 1}, {dumpID: 1, msgID: 2}, {dumpID: 2, msgID: 3},
	}
	var recovered []int
	n, err := deliverOrderedCollection(ctx, tracks, func(_ int64, _ []int) error {
		return errors.New("batch failed")
	}, func(i int) bool {
		recovered = append(recovered, i)
		cancel()
		return true
	})
	if !errors.Is(err, context.Canceled) || n != 1 || !reflect.DeepEqual(recovered, []int{0}) {
		t.Fatalf("delivered=%d, error=%v, recovered=%v", n, err, recovered)
	}
}

func TestCollectionDeliveryHonorsBatchLimit(t *testing.T) {
	tracks := make([]collectionDeliveryTrack, 181)
	for i := range tracks {
		tracks[i] = collectionDeliveryTrack{dumpID: 1, msgID: i + 1}
	}
	var sizes []int
	n, err := deliverOrderedCollection(context.Background(), tracks, func(_ int64, ids []int) error {
		sizes = append(sizes, len(ids))
		return nil
	}, func(i int) bool {
		t.Fatalf("unexpected fallback for track %d", i)
		return false
	})
	if err != nil || n != 181 || !reflect.DeepEqual(sizes, []int{90, 90, 1}) {
		t.Fatalf("delivered=%d, error=%v, batches=%v", n, err, sizes)
	}
}
