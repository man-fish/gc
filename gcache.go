package gc

import (
	"fmt"
	"log"
	"os"
	"sync"

	pb "github.com/man-fish/gc/gcachepb"
	"github.com/man-fish/gc/gcerror"
	"github.com/man-fish/gc/singleflight"
)

var (
	ErrGetter = errGetter()
	ErrKey    = errKey()
)

func errGetter() error { return gcerror.ErrGetter }
func errKey() error    { return gcerror.ErrKey }

// Getter loads data for a key
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc implements Getter
type GetterFunc func(key string) ([]byte, error)

// Get to get data
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// A Group is a cache namespace and associated data spread over
type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	logger    *log.Logger
	// loader make sure there will be only one fetch for a key
	loader *singleflight.Group
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// NewGroup create a new instance of Group
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic(ErrGetter)
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		mainCache: cache{cacheBytes: cacheBytes},
		getter:    getter,
		logger:    log.New(os.Stderr, fmt.Sprintf("[GCache %s]", name), log.LstdFlags),
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get a value from cache
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, ErrKey
	}
	if value, ok := g.mainCache.get(key); ok {
		g.logger.Printf("cache hit: %v", key)
		return value, nil
	}

	return g.load(key)
}

// RegisterPeers add remote peers to the group
func (g *Group) RegisterPeers(peers PeerPicker) {
	if peers == nil {
		g.logger.Panic("register peers should be more than one")
	}
	g.peers = peers
}

func (g *Group) load(key string) (value ByteView, err error) {
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.Pick(key); ok {
				if value, err = g.getFromPeer(peer, key); err != nil {
					return value, nil
				}
				g.logger.Printf("Fail to get from remote: %v", err)
			}
		}
		return g.getLocally(key)
	})
	if err != nil {
		return viewi.(ByteView), err
	}
	return
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}

func (g *Group) getLocally(key string) (ByteView, error) {
	v, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(v)}
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, val ByteView) {
	g.mainCache.add(key, val)
}
