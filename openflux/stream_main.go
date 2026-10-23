package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"universal-bypass-tool/socks5"
	"universal-bypass-tool/streamrelay"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/streamdocs"
	"universal-bypass-tool/transport/yandex"
)

func readStreamKey(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open stream key file")
	}
	defer f.Close()
	key, err := io.ReadAll(io.LimitReader(f, 33))
	if err != nil || len(key) != 32 {
		return nil, errors.New("stream key file must contain exactly 32 raw bytes")
	}
	return key, nil
}

func runStreamMode(docURLs []string, client bool, socksAddr, upstream, virtualAddress, keyPath string,
	chunkBytes, windowBytes, maxStreams, minReady int, readyTimeout, statsInterval, rto, stagger time.Duration, bootstrapPath string, transportConfig transport.TransportConfig) error {
	bootstrap, err := readDocumentBootstrap(bootstrapPath, docURLs)
	if err != nil {
		return err
	}
	key, err := readStreamKey(keyPath)
	if err != nil {
		return err
	}
	lanes := make([]transport.Transport, len(docURLs))
	for i, docURL := range docURLs {
		lane := yandex.NewYandexDocsTransport(docURL, transportConfig)
		if config, ok := bootstrap[docURL]; ok {
			if err := lane.SetBootstrapConfig(config.ClientConfig); err != nil {
				return err
			}
			if config.DialAddress != "" {
				if err := lane.SetBootstrapDialAddress(config.DialAddress); err != nil {
					return err
				}
			}
		}
		lane.SetChannelIndex(i + 1)
		lanes[i] = lane
	}
	trans, err := streamdocs.New(lanes, minReady, stagger)
	if err != nil {
		return err
	}
	cfg := streamrelay.Config{Key: key, ChunkSize: chunkBytes, WindowBytes: windowBytes,
		MaxStreams: maxStreams, MaxBufferedBytes: 64 << 20, Upstream: upstream, VirtualAddress: virtualAddress, RetransmitTimeout: rto}
	var relay *streamrelay.Relay
	if client {
		relay, err = streamrelay.NewClient(trans, cfg)
	} else {
		relay, err = streamrelay.NewFixedExit(trans, cfg)
	}
	if err != nil {
		return err
	}
	defer relay.Close()
	defer trans.Stop()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := trans.Start(); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(statsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Printf("StreamRelay ready=%d/%d %+v", trans.ReadyCount(), trans.Size(), relay.Stats())
			}
		}
	}()
	if err := waitAuthenticated(ctx, trans, readyTimeout, 250*time.Millisecond); err != nil {
		return err
	}
	log.Printf("Stream relay authenticated lanes=%d chunk_bytes=%d window_bytes=%d rto=%s", len(lanes), chunkBytes, windowBytes, rto)
	if !client {
		log.Print("Restricted stream exit ready")
		return monitorTransport(ctx, trans, nil, nil, 250*time.Millisecond)
	}
	server := socks5.NewSOCKS5Server(socksAddr, relay)
	defer server.Close()
	result := make(chan error, 1)
	go func() { result <- server.Start() }()
	ready := func() bool {
		if addr := server.Addr(); addr != nil {
			log.Printf("Local stream adapter ready on %s", addr)
			return true
		}
		return false
	}
	return monitorTransport(ctx, trans, result, ready, 250*time.Millisecond)
}
