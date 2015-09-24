//package drproxy
package main

import (
	//"fmt"
	//_ "html"
	"log"
	//"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	//"runtime"
	//"errors"
	//"fmt"
	_ "net/http/pprof"
	//"strings"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var responseChanPool sync.Pool

type empty struct{}

type semaphore chan empty

// Create new semaphore
func newSemaphore(n int) semaphore {
	return make(semaphore, n)
}

// acquire n resources
func (s semaphore) p(n int) {
	e := empty{}
	for i := 0; i < n; i++ {
		s <- e
	}
}

// release n resources
func (s semaphore) v(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}

// release a resource
type proxyList struct {
	list     []*httputil.ReverseProxy
	algoFlag bool
	q        []int
	qidx     int
	idx      int
	// rateLimiter channel
	rateLimiter semaphore
	//rateLimiter chan bool
}

func (pl *proxyList) putIdx(i int) {
	pl.qidx++
	if pl.qidx >= len(pl.q) {
		pl.q = append(pl.q, i)
	} else {
		pl.q[pl.qidx] = i
	}
	// TODO remove this in production
	//log.Printf("DEBUG incoming index = %d, pl.idx = %d, pl.qidx = %d\n", i, pl.idx, pl.qidx)
}

func (pl *proxyList) getIdx() int {

	result := -1
	// pl.qidx should never be < -1
	if pl.qidx < 0 {
		// roundrobin
		pl.idx = (pl.idx + 1) % len(pl.list)
		result = pl.idx
	} else {
		// MRU
		result = pl.q[pl.qidx]
		pl.qidx--
	}
	// TODO remove this in production
	//log.Printf("DEBUG returning index = %d, pl.idx = %d, pl.qidx = %d\n", result, pl.idx, pl.qidx)
	return result
}

type recentlyUsedRProxy struct {
	idx int
	pl  *proxyList
}

/*
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}
*/

type urlProxyMap map[string]*proxyList

type response struct {
	//proxy             *httputil.ReverseProxy
	proxy *proxyList
	idx   int
}

type chanIn struct {
	// origin host url
	// Set by the producer
	ohost string
	r     chan response
}

type perHostCounter struct {
	req     uint32
	waitReq uint32
}

type stats struct {
	// global requests waiting
	waitReq uint32
	// global requests currently under process
	reqCount uint32

	counter map[string]*perHostCounter
	lock    sync.Mutex
	// Frequency after which stats are dumped to the log file
	logFrequency time.Duration
}

var st *stats

// DRProxy is Dynamic Reverse Proxy
type DRProxy struct {
	m              urlProxyMap
	originHdr      string
	size           int
	maxConn        int
	maxConnPerHost int
	//lock      sync.RWMutex
	inCh              chan chanIn
	timeout           time.Duration
	globalRateLimiter semaphore
	//globalRateLimiter chan bool
	chRURP chan recentlyUsedRProxy
	// time after which a goroutine(per ohost) would give stats about global and perHost waitReqCount
}

// New Creates a new Dynamic Reverse Proxy
func New(hdr string, size int, maxConn int, maxConnPerHost int, bufSize int, timeout time.Duration, logFrequency time.Duration) *DRProxy {

	log.Printf("DEBUG drproxy.New() called with hdr = %s, size = %d, maxConn = %d, maxConnPerHost = %d, bufSize = %d, timeout = %d, logFrequency = %d\n", hdr, size, maxConn, maxConnPerHost, bufSize, timeout, logFrequency)

	responseChanPool.New = func() interface{} {
		return make(chan response)
	}

	dp := &DRProxy{originHdr: hdr, size: size, maxConn: maxConn, maxConnPerHost: maxConnPerHost, inCh: make(chan chanIn, bufSize), chRURP: make(chan recentlyUsedRProxy, bufSize), globalRateLimiter: newSemaphore(maxConn), m: make(urlProxyMap), timeout: timeout}

	// Create stats object
	st = &stats{logFrequency: logFrequency, counter: make(map[string]*perHostCounter)}
	// Spawn the consumer goroutine
	go func() {
		var (
			plist *proxyList
			err   error
			input chanIn
			rurp  recentlyUsedRProxy
		)
		for {
			// Get an input
			select {

			case rurp = <-dp.chRURP:
				{
					// TODO remove this in production
					//log.Printf("DEBUG chRURP received %+v\n", rurp)
					rurp.pl.putIdx(rurp.idx)
					continue
				}

			case input = <-dp.inCh:
				// TODO remove this in production
				//log.Printf("DEBUG Goroutine received %s\n", input.ohost)

			}

			plist = dp.m[input.ohost]
			if plist == nil {
				//TODO
				log.Printf("DEBUG Consumer goroutine received %s\n", input.ohost)
				// TODO(@kartik.mahajan):
				// Create a new proxyList
				if plist, err = newProxyList(dp.size, dp.originHdr, input.ohost, dp.maxConnPerHost, dp.maxConn, dp.timeout, st); err != nil {
					log.Printf("ERROR newProxyList() failed: %s", err.Error())
					input.r <- response{proxy: nil}
					continue
				}
				dp.m[input.ohost] = plist
			}
			//input.r <- response{proxy: plist, rateLimiter: plist.rateLimiter, idx: plist.getIdx()}
			input.r <- response{proxy: plist, idx: plist.getIdx()}
		}
	}()
	return dp
}

/*
func getRateLimiter(dp *DRProxy, ohost string, plist *proxyList) int {
	// Just return the highest capacity channel....dumb algorithm...
	//maxCap := cap(plist.rateLimiters[len(plist.rateLimiters) -1])
	return len(plist.rateLimiters) - 1
	// Get the capacity of the current channel
	idx := plist.currRateLimiter

	minCap := cap(plist.rateLimiters[0])
	currCap := cap(plist.rateLimiters[idx])
	maxCap := cap(plist.rateLimiters[len(plist.rateLimiters) -1])

	if currCap >= maxCap || dp.getWaitReqCount(ohost) <= currCap {
		// Nothing to do since we already reached the max connection limit for this ohost OR
		// the current channel buffer is enough
		return idx
	}
	// What algo should we use to determine the next channel to throw?
	// If we use the waitList for this ohost and globalWaitList....then how do we tell on subsequent call that we don't need to throw a new channel now
}
*/

//func newProxyList(size int, hdr string, ohost string, maxConnPerHost int, maxConn int, responseHdrTimeout time.Duration, dp *DRProxy) (*proxyList, error) {
func newProxyList(size int, hdr string, ohost string, maxConnPerHost int, maxConn int, responseHdrTimeout time.Duration, st *stats) (*proxyList, error) {
	var sourceIP = "Source_ip"

	//NOTE
	log.Printf("DEBUG drproxy.newProxyList() called size = %d, hdr = %s, ohost = %s, maxConn = %d, maxConnPerHost = %d, responseHdrTimeout = %d\n", size, hdr, ohost, maxConn, maxConnPerHost, responseHdrTimeout)

	target, err := url.Parse(ohost)
	if err != nil {
		log.Printf("ERROR newProxyList() failed for size = %d and ohost = %s\n", size, ohost)
		return nil, err
	}
	// TODO remove this in production
	//log.Printf("DEBUG %+v\n", target)

	res := new(proxyList)

	res.list = make([]*httputil.ReverseProxy, size)
	res.q = make([]int, maxConn*2)

	res.idx = -1
	res.qidx = -1

	targetQuery := target.RawQuery
	director := func(req *http.Request) {
		// remove X-purl
		req.Header.Del(hdr)
		req.Header.Del(sourceIP)
		req.Host = target.Host
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		//req.URL.Path = singleJoiningSlash(target.Path, req.URL.Path)
		req.URL.Path = target.Path
		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
		// TODO remove this in production
		//log.Printf("DEBUG %+v\n", req.URL)
		//log.Printf("DEBUG %+v\n", req)
	}

	for i := range res.list {
		res.list[i] = &httputil.ReverseProxy{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					Timeout:   100 * time.Millisecond,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout:   10 * time.Second,
				MaxIdleConnsPerHost:   maxConn,
				ResponseHeaderTimeout: responseHdrTimeout,
			},
			Director: director,
		}
	}

	res.rateLimiter = newSemaphore(maxConnPerHost)
	//res.rateLimiter = make(chan bool, maxConnPerHost)
	go func(st *stats) {
		tick := time.Tick(st.logFrequency)
		for {
			<-tick
			log.Printf("APP:%d:%d:%d:%s:%d:%d\n", runtime.NumGoroutine(), st.getGlobalWaitReqCount(), st.getGlobalReqCount(), ohost, st.getWaitReqCount(ohost), st.getReqCount(ohost))
		}
	}(st)
	return res, nil
}

func (dp *DRProxy) getProxy(ohost string) response {

	// BUG(@ktk57): Can we do pooling here to remove the overhead of allocation/free?
	//ch := make(chan response)
	// NOTE:- This is taking lock, should we use a splice of pools and then randomly select one?
	ch := responseChanPool.Get().(chan response)
	defer responseChanPool.Put(ch)

	tmp := chanIn{ohost: ohost, r: ch}

	// inCh should have "proper size"
	// BUG(@ktk57): Do we need to calculate the time spent waiting here?
	// A counter-intuitive observation is that as the size of this buffered channel increases, throughput decreases and latency increases. I am baffled and so have kept the buffer size as only 10, for which I observed max performance
	dp.inCh <- tmp

	// TODO Calculate the waiting time here
	return <-tmp.r
	// Get a reverseproxy and rateLimiter chan

	/*
		// Increment the global and individual request count & "waiting" request count for this origin host
		// This is here to avoid noise due to above possibly blocking operations and since both these counter will be used by "worker" to increase decrease allocated connections to an origin host
		dp.incrReqCount(ohost)
		dp.incrWaitReqCount(ohost)
	*/

	// Check if it is ok to
	/*
		in := <-tmp.r
		in.rateLimiter <- true
		if in.proxy == nil {
			<-in.rateLimiter
			return nil, fmt.Errorf("ERROR getProxy() failed")
		}

		//NOTE
		//log.Printf("%+v\n", in)
		return in.proxy, nil
	*/
}

func (st *stats) incrReqCount(ohost string) {
	// Increment the global request count
	atomic.AddUint32(&st.reqCount, 1)
	// Find if there is an item for this key
	if _, ok := st.counter[ohost]; ok == false {
		// No item in map for this origin host
		// Insert an item
		st.lock.Lock()
		// We need to check again since multiple goroutines can satisfy the previous predicate *think*
		if _, ok := st.counter[ohost]; ok == false {
			st.counter[ohost] = new(perHostCounter)
		}
		st.lock.Unlock()
	}
	atomic.AddUint32(&((st.counter[ohost]).req), 1)
}

// This assumes that "key" exists
func (st *stats) decrReqCount(ohost string) {
	// Decrement the global wait request count
	atomic.AddUint32(&st.reqCount, ^uint32(0))
	atomic.AddUint32(&((st.counter[ohost]).req), ^uint32(0))
}

// This assumes that "key" exists
func (st *stats) getReqCount(ohost string) uint32 {
	return atomic.LoadUint32(&((st.counter[ohost]).req))
}

// This assumes that "key" exists
func (st *stats) getGlobalReqCount() uint32 {
	return atomic.LoadUint32(&(st.reqCount))
}

func (st *stats) incrWaitReqCount(ohost string) {
	// Increment the global wait request count
	atomic.AddUint32(&st.waitReq, 1)
	if _, ok := st.counter[ohost]; ok == false {
		// No item in map for this origin host
		// Insert an item
		st.lock.Lock()
		// We need to check again since multiple goroutines can satisfy the previous predicate *think*
		if _, ok := st.counter[ohost]; ok == false {
			st.counter[ohost] = new(perHostCounter)
		}
		st.lock.Unlock()
	}
	atomic.AddUint32(&((st.counter[ohost]).waitReq), 1)
}

// This assumes that "key" exists
func (st *stats) decrWaitReqCount(ohost string) {
	// Decrement the global wait request count
	atomic.AddUint32(&st.waitReq, ^uint32(0))
	atomic.AddUint32(&((st.counter[ohost]).waitReq), ^uint32(0))
}

// This assumes that "key" exists
func (st *stats) getGlobalWaitReqCount() uint32 {
	return atomic.LoadUint32(&(st.waitReq))
}

// This assumes that "key" exists
func (st *stats) getWaitReqCount(ohost string) uint32 {
	return atomic.LoadUint32(&((st.counter[ohost]).waitReq))
}

// ServeHTTP will
func (dp *DRProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Find the header X-purl i.e origin host
	//log.Printf("DEBUG drproxy.ServeHTTP() called\n")
	ohost := r.Header.Get(dp.originHdr)
	if ohost == "" {
		log.Printf("ERROR header %s is missing\n", dp.originHdr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	st.incrWaitReqCount(ohost)
	// Find the proxyList for this origin ohost
	res := dp.getProxy(ohost)
	rproxy, idx := res.proxy, res.idx
	if rproxy == nil {
		log.Printf("ERROR dp.getProxy() returned nil as reverseProxy\n")
		w.WriteHeader(http.StatusInternalServerError)
		//dp.decrWaitReqCount(ohost)
		//dp.decrReqCount(ohost)
		return
	}
	// TODO remove this in production
	//log.Printf("DEBUG getProxy response = %+v\n", res)

	// TODO remove/log this in production
	// Increment the global and individual request count & "waiting" request count for this origin host
	// Check if it is ok to write to this channel
	// BUG(@ktk57): Do I really need a channel here? Can I do the same without a channel?(lets say semaphore etc?) Will that be less expensive? Right now CPU consumption never reaches > 65% EVER
	//res.rateLimiter.p(1)
	rproxy.rateLimiter.p(1)
	//res.rateLimiter <- true
	dp.globalRateLimiter.p(1)
	//dp.globalRateLimiter <- true
	// TODO remove/log this in production
	//log.Printf("DEBUG : req_%s: %d\n", ohost, dp.getReqCount(ohost))
	//log.Printf("DEBUG : req_global: %d\n", dp.getReqCount(

	//res.proxy.list[res.idx].ServeHTTP(w, r)
	st.decrWaitReqCount(ohost)

	st.incrReqCount(ohost)
	rproxy.list[idx].ServeHTTP(w, r)

	dp.chRURP <- recentlyUsedRProxy{pl: rproxy, idx: idx}
	rproxy.rateLimiter.v(1)
	dp.globalRateLimiter.v(1)
	st.decrReqCount(ohost)
	//<-res.rateLimiter
	//<-dp.globalRateLimiter
	//dp.decrReqCount(ohost)
}

/*
func handler(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusFound)
		//w.Header().
		w.
		fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
	http.Redirect(w, r, "http://www.yahoo.com/", http.StatusMovedPermanently)
}

func printNG(n time.Duration) {
	tick := time.Tick(n)
	for {
		case <-tick:
			fmt.Printf("# of Goroutines: %d\n", runtime.NumGoroutine())
	}
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	http.Handle("/", drproxy.New()
	http.HandleFunc("/", handler)
	log.Println("Starting the server on port 8080")
	go printNG(time.Second * 1)
	log.Fatalln(http.ListenAndServe(":8080", nil))
	http.Handle
}
*/
