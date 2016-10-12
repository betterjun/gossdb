package ssdb

import (
	"fmt"
	"sync/atomic"
)

// NewPool should be the first function you call, and then use the pool to handle ssdb.
func NewPool(ip string, port int, password string, poolSize int32) (p *Pool, err error) {
	p = &Pool{
		ip: ip, port: port, password: password, poolSize: poolSize,
	}

	err = p.Open()
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Pool holds a collection of connections to ssdb.
type Pool struct {
	// Connections to the server.
	clients chan *Client
	// Server ip.
	ip string
	// Server port.
	port int
	// Password for server, if no authcation need, just set empty string.
	password string
	// The max connection number to server.
	poolSize int32
	// Current connection number to server.
	active int32
	// Pool is closed or not
	opened bool
}

// Open creates the channel for Client connections.
func (p *Pool) Open() (err error) {
	if p.poolSize < 1 {
		p.poolSize = 1
	}

	p.clients = make(chan *Client, p.poolSize)
	p.opened = true
	return nil
}

// Close closes the channel for Client connections.
func (p *Pool) Close() {
	if p.opened == true {
		close(p.clients)
		p.opened = false

		for c := range p.clients {
			c.Close()
		}
	}
}

// Get returns a free Client connection, if no connection is free, just blocking until the pool is closed.
func (p *Pool) Get() (c *Client) {
	for p.opened {
		select {
		case c = <-p.clients:
			return c
		default:
			p.gen()
		}
	}
	return nil
}

// Release releases a Client connection.
func (p *Pool) Release(c *Client) {
	if c.err != nil {
		c.Close()
		return
	}

	// the pool may be closed already.
	if p.opened == true {
		p.clients <- c
	} else {
		c.Close()
	}
}

// ServerAddress returns the server ip and port.
func (p *Pool) ServerAddress() string {
	return fmt.Sprintf("%s:%d", p.ip, p.port)
}

// Size returns the max number of client connection allowed.
func (p *Pool) Size() int32 {
	return p.poolSize
}

// ActiveConnection returns the active connection have created.
func (p *Pool) ActiveConnection() int32 {
	return p.active
}

func (p *Pool) gen() {
	active := atomic.LoadInt32(&p.active)
	// no lock here, so active may exceed the pool size.
	if active >= p.poolSize {
		return
	}

	c, err := Connect(p.ip, p.port)
	if err != nil {
		fmt.Printf("connect to server error : %v\n", err)
		return
	}

	atomic.AddInt32(&p.active, 1)
	if len(p.password) != 0 {
		err = c.Auth(p.password)
		if err != nil {
			fmt.Printf("auth server error : %v\n", err)
			return
		}
	}
	p.clients <- c
}
