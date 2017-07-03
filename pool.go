package conn

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultKeepAliveTimeout 默认超时时间
const (
	DefaultKeepAliveTimeout = time.Second * 60
)

// ManagedConn net.TCPConn
type ManagedConn struct {
	net.TCPConn
	idle uint32
}

// NewPool 获取一个新的连接池
func NewPool() ConnPool {
	return ConnPool{
		timeout: DefaultKeepAliveTimeout,
		// id -> ConnManager
		// id is like 1.2.3.4:90, the resolved remoteAddr
		container: make(map[string]*ConnManager),
		mu:        &sync.Mutex{},
	}
}

// ConnPool 一个连接池
type ConnPool struct {
	timeout   time.Duration
	container map[string]*ConnManager
	mu        *sync.Mutex
}

// SetKeepAliveTimeout  设置超时时间
func (p *ConnPool) SetKeepAliveTimeout(t time.Duration) {
	p.timeout = t
}

// Get 获取一个指定远程地址的连接
func (p *ConnPool) Get(remoteAddr string) (*ManagedConn, error) {
	return p.get(remoteAddr, 0)
}

// Get 带上时间获取一个指定地址的连接
func (p *ConnPool) GetTimeout(remoteAddr string, timeout time.Duration) (*ManagedConn, error) {
	return p.get(remoteAddr, timeout)
}

func (p *ConnPool) get(remoteAddr string, timeout time.Duration) (conn *ManagedConn, err error) {
	remoteAddr = ensurePort(remoteAddr)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", remoteAddr)
	if err != nil {
		return
	}
	id := tcpAddr.String()

	p.mu.Lock()
	mgr := p.container[id]
	if mgr == nil {
		mgr = NewConnManager()
		p.container[id] = mgr
	}
	p.mu.Unlock()

	mgr.Lock()
	defer mgr.Unlock()

	if len(mgr.conns) == 0 {
		conn, err = p.createConn(tcpAddr, timeout)
	} else {
		conn = p.getFreeConn(id)
		if conn == nil {
			conn, err = p.createConn(tcpAddr, timeout)
		} else {
			debug("reusing conn %p, idle changed to 0\n", conn)
			atomic.StoreUint32(&conn.idle, 0)
		}
	}
	return
}

// Remove 立即删除连接
func (p *ConnPool) Remove(conn *ManagedConn) {
	debug("remove conn %p\n", conn)
	id := conn.RemoteAddr().String()
	mgr := p.container[id]
	mgr.Lock()
	defer mgr.Unlock()
	p.remove(conn)
}

// Put 把连接放到连接池里，并在超时时间里移除
func (p *ConnPool) Put(conn *ManagedConn) {
	debug("put back conn %p, idle changed to 1\n", conn)
	atomic.StoreUint32(&conn.idle, 1)

	go func() {
		timer := time.NewTimer(p.timeout)
		<-timer.C

		id := conn.RemoteAddr().String()
		mgr := p.container[id]
		// Lock it
		mgr.Lock()
		defer mgr.Unlock()

		if atomic.LoadUint32(&conn.idle) == 0 {
			debug("conn %p is reused, skipping release\n", conn)
			return
		}
		p.remove(conn)
	}()
}

func (p *ConnPool) remove(conn *ManagedConn) {
	id := conn.RemoteAddr().String()
	mgr := p.container[id]
	idx := findIndex(mgr.conns, conn)
	if idx == -1 {
		// conn has already been released
		return
	}
	mgr.conns = append(mgr.conns[:idx], mgr.conns[idx+1:]...)
	conn.Close()
}

func (p ConnPool) createConn(tcpAddr *net.TCPAddr, timeout time.Duration) (conn *ManagedConn, err error) {
	var rawConn net.Conn
	if timeout == 0 {
		rawConn, err = net.Dial("tcp4", tcpAddr.String())
	} else {
		rawConn, err = net.DialTimeout("tcp4", tcpAddr.String(), timeout)
	}
	if err == nil {
		conn = &ManagedConn{
			TCPConn: *rawConn.(*net.TCPConn),
			idle:    0,
		}
		mgr := p.container[tcpAddr.String()]
		mgr.conns = append(mgr.conns, conn)
		debug("creating new conn: %p\n", conn)
	}
	return
}

func (p ConnPool) getFreeConn(id string) (c *ManagedConn) {
	for _, c = range p.container[id].conns {
		debug("scanning cann %p, idle: %v\n", c, c.idle)
		if atomic.LoadUint32(&c.idle) == 1 {
			debug("found free conn: %p\n", c)
			return
		}
	}
	c = nil // dont return the last one
	return
}

func findIndex(arr []*ManagedConn, ele *ManagedConn) int {
	for idx, v := range arr {
		if v == ele {
			return idx
		}
	}
	return -1
}

func ensurePort(addr string) (rv string) {
	rv = addr
	if !strings.Contains(addr, ":") {
		rv = fmt.Sprintf("%s:80", rv)
	}
	return
}
