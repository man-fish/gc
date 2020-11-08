package gc

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/man-fish/gc/consistenthash"
	pb "github.com/man-fish/gc/gcachepb"
)

type httpGetter struct {
	defaultURL string
}

func (g *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf("%v%v/%v", g.defaultURL, url.QueryEscape(in.Group), url.QueryEscape(in.Key))
	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server reponse with code: %v", resp.StatusCode)
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %v", err)
	}
	if err := proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

const (
	defaultBasePath = "/gc/"
	defaultReplicas = 50
)

// HTTPPool marked current gc info
type HTTPPool struct {
	self        string
	basepath    string
	mu          sync.Mutex
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter
	logger      *log.Logger
}

// NewHTTPPool is the constructor of HTTPPool
func NewHTTPPool(self string) *HTTPPool {
	logger := log.New(os.Stderr, fmt.Sprintf("[Server %s]", self), log.LstdFlags)
	return &HTTPPool{
		self:        self,
		basepath:    defaultBasePath,
		peers:       consistenthash.New(defaultReplicas, nil),
		httpGetters: make(map[string]*httpGetter),
		logger:      logger,
	}
}

// Set peers in pool
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers.Add(peers...)
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{defaultURL: peer + p.basepath}
	}
}

// Pick use to pick a peer from virtual node ring and return its Getter
func (p *HTTPPool) Pick(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.logger.Panicf("pick peer: %v", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

var _ PeerPicker = (*HTTPPool)(nil)

// ServerHTTP is a func to handle http req
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basepath) {
		p.logger.Panicln("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.logger.Printf("%s, %s\n", r.Method, r.URL.Path)
	// /<basepath>/<groupname>/<key> required
	parts := strings.SplitN(r.URL.Path[len(p.basepath):], "/", 2)

	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	bv, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := proto.Marshal(&pb.Response{Value: bv.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)
}
