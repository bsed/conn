package conn

import (
	"sync"
)

// ConnManager 连接管理
type ConnManager struct {
	sync.Mutex
	conns []*ManagedConn
}

// NewConnManager 连接管理器
func NewConnManager() *ConnManager {
	return &ConnManager{
		conns: make([]*ManagedConn, 0),
	}
}
