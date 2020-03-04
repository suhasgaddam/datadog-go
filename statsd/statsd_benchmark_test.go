package statsd_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
)

func setupUDSClientServer(b *testing.B, options []statsd.Option) (*statsd.Client, net.Listener) {
	sockAddr := "/tmp/test.sock"
	if err := os.RemoveAll(sockAddr); err != nil {
		log.Fatal(err)
	}
	conn, err := net.Listen("unix", sockAddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go func() {
		for {
			_, err := conn.Accept()
			if err != nil {
				return
			}
		}
	}()
	client, err := statsd.New("unix://"+sockAddr, options...)
	if err != nil {
		b.Error(err)
	}
	return client, conn
}

func setupUDPClientServer(b *testing.B, options []statsd.Option) (*statsd.Client, *net.UDPConn) {
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		b.Error(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		b.Error(err)
	}

	client, err := statsd.New(conn.LocalAddr().String(), options...)
	if err != nil {
		b.Error(err)
	}
	return client, conn
}

func setupClient(b *testing.B, transport string, sendingMode statsd.SendingMode) (*statsd.Client, io.Closer) {
	options := []statsd.Option{statsd.WithMaxMessagesPerPayload(1024), statsd.WithoutTelemetry()}
	if sendingMode == statsd.BlockOnSend {
		options = append(options, statsd.WithBlockOnSendMode())
	} else {
		options = append(options, statsd.WithDropOnSendMode())
	}

	if transport == "udp" {
		return setupUDPClientServer(b, options)
	}
	return setupUDSClientServer(b, options)
}

func benchmarkStatsdDifferentMetrics(b *testing.B, transport string, sendingMode statsd.SendingMode) {
	client, conn := setupClient(b, transport, sendingMode)
	defer conn.Close()

	n := int32(0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		testNumber := atomic.AddInt32(&n, 1)
		name := fmt.Sprintf("test.metric%d", testNumber)
		for pb.Next() {
			client.Gauge(name, 1, []string{"tag:tag"}, 1)
		}
	})
	client.Flush()
	t := client.FlushTelemetryMetrics()
	reportMetric(b, float64(t.TotalDroppedOnReceive)/float64(t.TotalMetrics)*100, "%_dropRate")

	b.StopTimer()
	client.Close()
}

func benchmarkStatsdSameMetrics(b *testing.B, transport string, sendingMode statsd.SendingMode) {
	client, conn := setupClient(b, transport, sendingMode)
	defer conn.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client.Gauge("test.metric", 1, []string{"tag:tag"}, 1)
		}
	})
	client.Flush()
	t := client.FlushTelemetryMetrics()
	reportMetric(b, float64(t.TotalDroppedOnReceive)/float64(t.TotalMetrics)*100, "%_dropRate")

	b.StopTimer()
	client.Close()
}

// UDP
func BenchmarkStatsdUDPSameMetricBLocking(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "udp", statsd.BlockOnSend)
}
func BenchmarkStatsdUDPSameMetricDropping(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "udp", statsd.DropOnSend)
}

func BenchmarkStatsdUDPDifferentMetricBlocking(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "udp", statsd.BlockOnSend)
}
func BenchmarkStatsdUDPDifferentMetricDropping(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "udp", statsd.DropOnSend)
}

// UDS
func BenchmarkStatsdUDSSameMetricBLocking(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "uds", statsd.BlockOnSend)
}
func BenchmarkStatsdUDSSameMetricDropping(b *testing.B) {
	benchmarkStatsdSameMetrics(b, "uds", statsd.DropOnSend)
}

func BenchmarkStatsdUDPSifferentMetricBlocking(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "uds", statsd.BlockOnSend)
}
func BenchmarkStatsdUDSDifferentMetricDropping(b *testing.B) {
	benchmarkStatsdDifferentMetrics(b, "uds", statsd.DropOnSend)
}
