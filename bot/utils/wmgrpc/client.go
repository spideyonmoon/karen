package wmgrpc

import (
	"context"
	"fmt"
	"time"

	pb "github.com/WorldObservationLog/wrapper-manager/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.WrapperManagerServiceClient
	id     string
}

// ID returns the stable identifier assigned at construction (e.g. "wm-1").
// Empty when NewClient was called without an id (legacy callers / tests).
func (c *Client) ID() string {
	if c == nil {
		return ""
	}
	return c.id
}

func NewClient(addr string) (*Client, error) {
	return NewClientWithID(addr, "")
}

func NewClientWithID(addr, id string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("wmgrpc: dial %s: %w", addr, err)
	}
	c := &Client{
		conn:   conn,
		client: pb.NewWrapperManagerServiceClient(conn),
		id:     id,
	}
	return c, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Status(ctx context.Context) (*pb.StatusData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := c.client.Status(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("wmgrpc Status: %w", err)
	}
	if resp.Header.Code != 0 {
		return nil, fmt.Errorf("wmgrpc Status: %s", resp.Header.Msg)
	}
	return resp.Data, nil
}

func (c *Client) M3U8(ctx context.Context, adamID string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.client.M3U8(ctx, &pb.M3U8Request{
		Data: &pb.M3U8DataRequest{AdamId: adamID},
	})
	if err != nil {
		return "", fmt.Errorf("wmgrpc M3U8: %w", err)
	}
	if resp.Header.Code != 0 {
		return "", fmt.Errorf("wmgrpc M3U8: %s", resp.Header.Msg)
	}
	return resp.Data.M3U8, nil
}

func (c *Client) WebPlayback(ctx context.Context, adamID string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.client.WebPlayback(ctx, &pb.WebPlaybackRequest{
		Data: &pb.WebPlaybackDataRequest{AdamId: adamID},
	})
	if err != nil {
		return "", fmt.Errorf("wmgrpc WebPlayback: %w", err)
	}
	if resp.Header.Code != 0 {
		return "", fmt.Errorf("wmgrpc WebPlayback: %s", resp.Header.Msg)
	}
	return resp.Data.M3U8, nil
}

type DecryptionStream struct {
	stream pb.WrapperManagerService_DecryptClient
	cancel context.CancelFunc
	adamID string
}

func (c *Client) NewDecryptionStream(ctx context.Context, adamID string) (*DecryptionStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.client.Decrypt(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("wmgrpc Decrypt stream: %w", err)
	}
	return &DecryptionStream{stream: stream, cancel: cancel, adamID: adamID}, nil
}

func (ds *DecryptionStream) Decrypt(key string, sample []byte, sampleIndex int32) ([]byte, error) {
	err := ds.stream.Send(&pb.DecryptRequest{
		Data: &pb.DecryptData{
			AdamId:      ds.adamID,
			Key:         key,
			SampleIndex: sampleIndex,
			Sample:      sample,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("send sample %d: %w", sampleIndex, err)
	}
	resp, err := ds.stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv sample %d: %w", sampleIndex, err)
	}
	if resp.Header.Code != 0 {
		return nil, fmt.Errorf("decrypt sample %d: %s", sampleIndex, resp.Header.Msg)
	}
	return resp.Data.Sample, nil
}

func (ds *DecryptionStream) Close() {
	ds.cancel()
	_ = ds.stream.CloseSend()
}

func (c *Client) License(ctx context.Context, adamID, challenge, uri string) (string, error) {
	resp, err := c.client.License(ctx, &pb.LicenseRequest{
		Data: &pb.LicenseDataRequest{
			AdamId:    adamID,
			Challenge: challenge,
			Uri:       uri,
		},
	})
	if err != nil {
		return "", fmt.Errorf("wmgrpc License: %w", err)
	}
	if resp.Header.Code != 0 {
		return "", fmt.Errorf("wmgrpc License: %s", resp.Header.Msg)
	}
	return resp.Data.License, nil
}

func (c *Client) Lyrics(ctx context.Context, adamID, region, language string) (string, error) {
	resp, err := c.client.Lyrics(ctx, &pb.LyricsRequest{
		Data: &pb.LyricsDataRequest{
			AdamId:   adamID,
			Region:   region,
			Language: language,
		},
	})
	if err != nil {
		return "", fmt.Errorf("wmgrpc Lyrics: %w", err)
	}
	if resp.Header.Code != 0 {
		return "", fmt.Errorf("wmgrpc Lyrics: %s", resp.Header.Msg)
	}
	return resp.Data.Lyrics, nil
}
