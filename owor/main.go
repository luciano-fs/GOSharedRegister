package main

import (
    "sync"
    "log"
    "math"
    "context"
)

type Request struct {
    sender chan<- Response
    write  bool
    value  int
    seq    int
}

func (r Request) GetSender() chan<- Response {
    return r.sender
}

func (r Request) GetType() bool {
    return r.write
}

func (r Request) GetValue() int {
    return r.value
}

func (r Request) GetSeq() int {
    return r.seq
}

type Response struct {
    value int
    seq   int
}

func (r Response) GetValue() int {
    return r.value
}

func (r Response) GetSeq() int {
    return r.seq
}

type server struct {
    value int
    seq   int
    in    <-chan Request
}

type client struct {
    value int
    seq   int
    out   map[int]chan<- Request
    in    chan Response
}

func (c *client) read() {
    n := len(c.out)
    f := n/2
    resp := make(chan Response, n)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
	for _, s := range c.out {
		go func(s chan<- Request, resp chan<- Response, ctx context.Context) {
            select {
                case s <- Request{
                    sender: resp,
                    write:  false,
                    value:  0,
                    seq:    0,
                }:

                case <- ctx.Done():
            }
		}(s, resp, ctx)
	}

    seq := math.MinInt32
    value := 0

    for i := 0; i < n-f; i++ {
        cur := <-resp
        if cur.GetSeq() > seq {
            seq = cur.GetSeq()
            value = cur.GetValue()
        }
    }

    c.seq = seq
    c.value = value
}

func (c *client) write(value int) {
    n := len(c.out)
    f := n/2
    resp := make(chan Response, n)
    c.seq++
    c.value = value

	for _, s := range c.out {
		go func(s chan<- Request, resp chan<- Response, value int, seq int) {
            s <- Request{
                sender: resp,
                write:  true,
                value:  value,
                seq:    seq,
            }
		}(s, resp, c.value, c.seq)
	}

    for i := 0; i < n-f; i++ {
        <-resp
    }
}

func (s *server) start() {
    for {
        req := <-s.in
        if req.GetType() == true && req.GetSeq() > s.seq {
            s.seq = req.GetSeq()
            s.value = req.GetValue()
        }
        req.sender <- Response {
            value: s.value,
            seq:   s.seq,
        }
    }
}

func main() {
    var wg sync.WaitGroup

    n := 10

    outchans := make(map[int]chan<- Request, n)
    servers := make([]server, n)
    for i := 0; i < n; i++ {
        ch := make(chan Request)
        outchans[i] = ch
        servers[i] = server{
            in:    ch,
            value: 0,
            seq:   0,
        }
    }

    w := &client {
        value: -1,
        seq: 0,
        in:  make(chan Response),
        out: outchans,
    }

    c1 := &client {
        value: -1,
        seq: 0,
        in:  make(chan Response),
        out: outchans,
    }

    c2 := &client {
        value: -1,
        seq: 0,
        in:  make(chan Response),
        out: outchans,
    }

	for _, s := range servers {
		go func(s server) {
            s.start()
		}(s)
	}

    wg.Add(3)

    go func(w *client, wg *sync.WaitGroup) {
        for i:=1; i<100; i++ {
            w.write(i)
            log.Println("Wrote ", i)
        }
        wg.Done()
    }(w, &wg)

    go func(c *client, wg *sync.WaitGroup) {
        for i:=1; i<100; i++ {
            c.read()
            log.Println("c1 read ", c1.value)
        }
        wg.Done()
    }(c1, &wg)

    go func(c *client, wg *sync.WaitGroup) {
        for i:=1; i<100; i++ {
            c.read()
            log.Println("c1 read ", c1.value)
        }
        wg.Done()
    }(c2, &wg)

    wg.Wait()
}
