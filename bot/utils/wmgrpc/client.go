package wmgrpc

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/WorldObservationLog/wrapper-manager/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.WrapperManagerServiceClient
}

func NewClient(addr string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("wmgrpc: dial %s: %w", addr, err)
	}
	c := &Client{
		conn:   conn,
		client: pb.NewWrapperManagerServiceClient(conn),
	}
	return c, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Status(ctx context.Context) (*pb.StatusData, error) {
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

func (c *Client) DecryptSample(ctx context.Context, adamID, key string, sample []byte, sampleIndex int32) ([]byte, error) {
	stream, err := c.client.Decrypt(ctx)
	if err != nil {
		return nil, fmt.Errorf("wmgrpc Decrypt stream: %w", err)
	}
	defer stream.CloseSend()
	err = stream.Send(&pb.DecryptRequest{
		Data: &pb.DecryptData{
			AdamId:      adamID,
			Key:         key,
			SampleIndex: sampleIndex,
			Sample:      sample,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("wmgrpc Decrypt send: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("wmgrpc Decrypt recv: %w", err)
	}
	if resp.Header.Code != 0 {
		return nil, fmt.Errorf("wmgrpc Decrypt: %s", resp.Header.Msg)
	}
	return resp.Data.Sample, nil
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

type DecryptResult struct {
	SampleIndex int32
	Data        []byte
	Err         error
}

func (c *Client) DecryptSamples(ctx context.Context, adamID string, keys []string, samples [][]byte, sampleIndices []int32) ([]DecryptResult, error) {
	stream, err := c.client.Decrypt(ctx)
	if err != nil {
		return nil, fmt.Errorf("wmgrpc DecryptStream: %w", err)
	}
	defer stream.CloseSend()

	type pending struct {
		idx   int
		key   string
		data  []byte
		sidx  int32
	}

	sendCh := make(chan pending, len(samples))
	results := make([]DecryptResult, len(samples))

	sendDone := make(chan error, 1)
	go func() {
		defer close(sendDone)
		for p := range sendCh {
			err := stream.Send(&pb.DecryptRequest{
				Data: &pb.DecryptData{
					AdamId:      adamID,
					Key:         p.key,
					SampleIndex: p.sidx,
					Sample:      p.data,
				},
			})
			if err != nil {
				sendDone <- fmt.Errorf("send sample %d: %w", p.idx, err)
				return
			}
		}
	}()

	sendCh <- pending{idx: 0, key: keys[0], data: samples[0], sidx: sampleIndices[0]}
	for i := 1; i < len(samples); i++ {
		sendCh <- pending{idx: i, key: keys[i], data: samples[i], sidx: sampleIndices[i]}
	}

	for i := 0; i < len(samples); i++ {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return results, fmt.Errorf("wmgrpc DecryptStream recv %d: %w", i, err)
		}
		if resp.Header.Code != 0 {
			results[i] = DecryptResult{SampleIndex: resp.Data.SampleIndex, Err: fmt.Errorf("decrypt failed: %s", resp.Header.Msg)}
		} else {
			results[i] = DecryptResult{SampleIndex: resp.Data.SampleIndex, Data: resp.Data.Sample}
		}
	}

	close(sendCh)
	if err := <-sendDone; err != nil {
		return results, err
	}
	return results, nil
}
