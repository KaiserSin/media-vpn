package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"universal-bypass-tool/socks5"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/docpool"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Media transport stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	exitNode := flag.Bool("exit-node", false, "Run the restricted loopback exit")
	client := flag.Bool("client", false, "Run the local client")
	socksAddr := flag.String("socks5", "127.0.0.1:19080", "Loopback SOCKS5 listener")
	urlFile := flag.String("url-file", "", "File containing the shared test document URL")
	urlListFile := flag.String("url-list-file", "", "File containing the same 1 to 10 shared document URLs on both sides, one per line")
	poolMode := flag.String("pool-mode", "flow", "Document pool scheduling: flow or stripe")
	poolNamespace := flag.String("pool-namespace", "", "Optional isolated experiment namespace; identical on both peers")
	batchBytes := flag.Int("batch-bytes", 0, "Optional document message batching limit in bytes (0 disables, max 32768)")
	batchDelay := flag.Duration("batch-delay", time.Millisecond, "Maximum batching delay (0 to 2ms)")
	congestion := flag.String("congestion", "reno", "Virtual TCP congestion control: reno or cubic")
	streamRelay := flag.Bool("stream-relay", false, "Experimental reliable byte streams over documents, without virtual TCP")
	docBootstrapFile := flag.String("doc-bootstrap-file", "", "Optional private browser-captured document bootstrap bundle (stream mode only)")
	streamKeyFile := flag.String("stream-key-file", "", "Private file containing exactly32 raw bytes shared by both stream peers")
	streamChunkBytes := flag.Int("stream-chunk-bytes", 1024, "Byte-stream DATA frame payload size")
	streamWindowBytes := flag.Int("stream-window-bytes", 4<<20, "Maximum unacknowledged byte-stream data per connection")
	streamRTO := flag.Duration("stream-rto", 750*time.Millisecond, "Minimum stream retransmission timeout")
	minReady := flag.Int("min-ready", 0, "Pool initial ready requirement; zero means all documents")
	startStagger := flag.Duration("start-stagger", 350*time.Millisecond, "Delay between document channel starts")
	queuePackets := flag.Int("queue-packets", 128, "Bounded send queue packets per document (1 to 256)")
	statsInterval := flag.Duration("stats-interval", 10*time.Second, "Pool counters log interval")
	virtualIP := flag.String("virtual-ip", "10.203.0.1", "Virtual destination inside the userspace tunnel")
	virtualPort := flag.Uint("virtual-port", 443, "Virtual TCP destination port")
	upstream := flag.String("upstream", "127.0.0.1:443", "Fixed loopback TLS upstream (exit only)")
	maxConnections := flag.Int("max-connections", 32, "Maximum simultaneous fixed-upstream connections on the exit (1 to 1024)")
	readyTimeout := flag.Duration("ready-timeout", 60*time.Second, "Maximum initial authentication time")
	flag.Parse()
	if *client == *exitNode {
		return errors.New("select exactly one of --client or --exit-node")
	}
	if err := validateBootstrapMode(*docBootstrapFile, *streamRelay); err != nil {
		return err
	}
	if *virtualPort == 0 || *virtualPort > 65535 {
		return errors.New("invalid virtual port")
	}
	if *maxConnections < 1 || *maxConnections > 1024 {
		return errors.New("max-connections must be between 1 and 1024")
	}
	if *client {
		host, _, err := net.SplitHostPort(*socksAddr)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return errors.New("SOCKS5 must listen on a literal loopback address")
		}
	}
	docURLs, err := readDocumentURLs(*urlFile, *urlListFile, os.Getenv("MEDIA_DOCUMENT_URL"))
	if err != nil {
		return err
	}
	poolID, err := poolIdentity(docURLs, *poolNamespace)
	if err != nil {
		return err
	}
	if *poolNamespace != "" && *urlListFile == "" {
		return errors.New("pool-namespace requires url-list-file")
	}
	if *readyTimeout <= 0 {
		return errors.New("ready-timeout must be positive")
	}
	cfg := transport.DefaultConfig()
	cfg.MaxReconnectAttempts = 8
	cfg.ReconnectDelay = time.Second
	cfg.ReconnectMultiplier = 2
	if *queuePackets < 1 || *queuePackets > 256 {
		return errors.New("queue-packets must be between 1 and 256")
	}
	if *statsInterval < time.Second {
		return errors.New("stats-interval must be at least one second")
	}
	cfg.MaxQueueSize = *queuePackets
	if *streamRelay {
		if *batchBytes != 0 {
			return errors.New("stream-relay frames data itself; batch-bytes must be zero")
		}
		return runStreamMode(docURLs, *client, *socksAddr, *upstream,
			fmt.Sprintf("%s:%d", *virtualIP, *virtualPort), *streamKeyFile,
			*streamChunkBytes, *streamWindowBytes, *maxConnections, *minReady,
			*readyTimeout, *statsInterval, *streamRTO, *startStagger, *docBootstrapFile, cfg)
	}
	var trans runningTransport
	var pool *docpool.Pool
	var documentLanes []*yandex.YandexDocsTransport
	if *urlListFile != "" {
		lanes := make([]transport.Transport, len(docURLs))
		for i, docURL := range docURLs {
			lane := yandex.NewYandexDocsTransport(docURL, cfg)
			if err := lane.ConfigureBatching(*batchBytes, *batchDelay); err != nil {
				return err
			}
			lane.SetChannelIndex(i + 1)
			documentLanes = append(documentLanes, lane)
			lanes[i] = lane
		}
		pool, err = docpool.New(lanes, docpool.Config{Mode: *poolMode, MinReady: *minReady, StartStagger: *startStagger, PoolID: poolID})
		if err != nil {
			return err
		}
		trans = pool
	} else {
		lane := yandex.NewYandexDocsTransport(docURLs[0], cfg)
		if err := lane.ConfigureBatching(*batchBytes, *batchDelay); err != nil {
			return err
		}
		documentLanes = append(documentLanes, lane)
		trans = lane
	}
	var tun *tunnel.TCPTunnel
	if *exitNode {
		tun, err = tunnel.NewFixedExitTunnel(trans, tunnel.FixedExitConfig{
			VirtualIP: *virtualIP, VirtualPort: uint16(*virtualPort), Upstream: *upstream,
			MaxConnections: *maxConnections, DialTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute,
		})
	} else {
		tun, err = tunnel.NewClientTunnel(trans)
	}
	if err != nil {
		return fmt.Errorf("initialize TCP adapter: %w", err)
	}
	defer tun.Close()
	defer trans.Stop()
	if err := tun.SetCongestionControl(*congestion); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := trans.Start(); err != nil {
		return fmt.Errorf("start document transport: %w", err)
	}
	log.Printf("Experiment batching_bytes=%d batching_delay=%s congestion=%s isolated=%t", *batchBytes, *batchDelay, *congestion, *poolNamespace != "")
	go logSpeedStats(ctx, tun, documentLanes, *statsInterval)
	if pool != nil {
		log.Printf("Document pool starting channels=%d mode=%s min_ready=%d", pool.Size(), *poolMode, pool.MinReady())
		go logPoolStats(ctx, pool, *statsInterval)
	}
	if *exitNode {
		go logExitStats(ctx, tun, *statsInterval)
	}
	if err := waitAuthenticated(ctx, trans, *readyTimeout, 250*time.Millisecond); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		if pool != nil {
			return fmt.Errorf("document pool authentication %d/%d ready: %w", pool.ReadyCount(), pool.Size(), err)
		}
		return err
	}
	log.Print("Media document transport authenticated")
	if *exitNode {
		log.Print("Restricted TLS exit ready")
		return monitorTransport(ctx, trans, nil, nil, 250*time.Millisecond)
	}
	server := socks5.NewSOCKS5Server(*socksAddr, tun)
	defer server.Close()
	result := make(chan error, 1)
	go func() { result <- server.Start() }()
	ready := func() bool {
		if addr := server.Addr(); addr != nil {
			log.Printf("Local adapter ready on %s", addr)
			return true
		}
		return false
	}
	return monitorTransport(ctx, trans, result, ready, 250*time.Millisecond)
}

func poolIdentity(urls []string, namespace string) ([16]byte, error) {
	var id [16]byte
	if len(namespace) > 64 {
		return id, errors.New("pool-namespace exceeds 64 characters")
	}
	for _, c := range namespace {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return id, errors.New("pool-namespace accepts only ASCII letters, numbers, dot, dash and underscore")
		}
	}
	seed := strings.Join(urls, "\n")
	if namespace != "" {
		seed = "media-experiment\x00" + namespace + "\x00" + seed
	}
	digest := sha256.Sum256([]byte(seed))
	copy(id[:], digest[:16])
	return id, nil
}

func logSpeedStats(ctx context.Context, tun *tunnel.TCPTunnel, lanes []*yandex.YandexDocsTransport, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("VirtualTCP %+v", tun.TCPStats())
			for i, lane := range lanes {
				log.Printf("Batch channel=%02d %+v", i+1, lane.BatchStats())
			}
		}
	}
}

func logExitStats(ctx context.Context, tun *tunnel.TCPTunnel, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s := tun.ExitStats()
			log.Printf("Exit active=%d limit=%d accepted=%d rejected_limit=%d dial_failed=%d", s.Active, s.Limit, s.Accepted, s.RejectedLimit, s.DialFailed)
		}
	}
}

type runningTransport interface {
	transport.Transport
	IsRunning() bool
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open document URL file")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, errors.New("cannot read document URL file")
	}
	if int64(len(b)) > limit {
		return nil, errors.New("document URL file too large")
	}
	return b, nil
}

func readDocumentURLs(singlePath, listPath, envURL string) ([]string, error) {
	if singlePath != "" && listPath != "" {
		return nil, errors.New("use only one of url-file and url-list-file")
	}
	if listPath != "" {
		b, err := readBoundedFile(listPath, 10*8193)
		if err != nil {
			return nil, err
		}
		var urls []string
		seen := make(map[string]bool)
		for _, line := range strings.Split(string(b), "\n") {
			u := strings.TrimSpace(line)
			if u == "" {
				continue
			}
			if len(u) > 8192 || seen[u] {
				return nil, errors.New("document list contains an oversized or duplicate entry")
			}
			seen[u] = true
			urls = append(urls, u)
			if len(urls) > docpool.MaxLanes {
				return nil, errors.New("document list exceeds 10 entries")
			}
		}
		if len(urls) == 0 {
			return nil, errors.New("document list is empty")
		}
		sort.Strings(urls)
		return urls, nil
	}
	u := strings.TrimSpace(envURL)
	if singlePath != "" {
		b, err := readBoundedFile(singlePath, 8192)
		if err != nil {
			return nil, err
		}
		u = strings.TrimSpace(string(b))
	}
	if u == "" {
		return nil, errors.New("provide url-file, url-list-file or MEDIA_DOCUMENT_URL")
	}
	if len(u) > 8192 {
		return nil, errors.New("document URL too large")
	}
	return []string{u}, nil
}

func logPoolStats(ctx context.Context, pool *docpool.Pool, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("Pool ready=%d/%d required=%d", pool.ReadyCount(), pool.Size(), pool.MinReady())
			for _, l := range pool.LaneStats() {
				log.Printf("Channel %02d ready=%t running=%t accepted_bytes=%d sent_bytes=%d received_bytes=%d send_errors=%d reconnects=%d", l.Index, l.Connected, l.Running, l.AcceptedBytes, l.Transport.BytesSent, l.Transport.BytesReceived, l.SendErrors, l.Transport.Reconnects)
			}
		}
	}
}

type transportState interface {
	IsConnected() bool
	IsRunning() bool
}

func waitAuthenticated(ctx context.Context, state transportState, timeout, poll time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !state.IsRunning() {
			return errors.New("document transport stopped before authentication")
		}
		if state.IsConnected() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errors.New("document transport did not authenticate before timeout")
		case <-ticker.C:
		}
	}
}

// A terminal transport failure must stop the process so a service manager can
// restart it. Temporary disconnections while reconnecting are allowed.
func monitorTransport(ctx context.Context, state transportState, result <-chan error, ready func() bool, poll time.Duration) error {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if !state.IsRunning() {
			return errors.New("document transport stopped after retry limit")
		}
		if ready != nil && ready() {
			ready = nil
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-result:
			return err
		case <-ticker.C:
		}
	}
}
