package wmgrpc

import "fmt"

// Pool holds one *Client per wrapper-manager instance and distributes them
// via a buffered channel. Acquire blocks until a client is free; Release
// returns it. This gives FIFO scheduling with natural backpressure at zero
// complexity cost.
type Pool struct {
	ch      chan *Client
	clients []*Client
}

// NewPool dials every address in addrs and returns a ready Pool.
// All connections are established eagerly so startup fails fast on misconfiguration.
func NewPool(addrs []string) (*Pool, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("wmgrpc: pool requires at least one address")
	}
	p := &Pool{
		ch:      make(chan *Client, len(addrs)),
		clients: make([]*Client, 0, len(addrs)),
	}
	for _, addr := range addrs {
		c, err := NewClient(addr)
		if err != nil {
			// Best-effort close already-dialled clients before returning error.
			for _, existing := range p.clients {
				_ = existing.Close()
			}
			return nil, fmt.Errorf("wmgrpc: pool dial %s: %w", addr, err)
		}
		p.clients = append(p.clients, c)
		p.ch <- c // all clients start available
	}
	return p, nil
}

// Acquire blocks until a client is available and returns it.
// The caller MUST call Release when done.
func (p *Pool) Acquire() *Client {
	return <-p.ch
}

// Release returns a client to the pool.
func (p *Pool) Release(c *Client) {
	p.ch <- c
}

// Close closes all underlying gRPC connections.
// Do not call Acquire after Close.
func (p *Pool) Close() {
	for _, c := range p.clients {
		_ = c.Close()
	}
}
