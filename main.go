// package main
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var (
	maxConnsPerHost     = 0
	maxIdleConnsPerHost = 100

	// MiB means 1024 KiB.
	MiB = int64(1024 * 1024)

	numOfWorkers = flag.Int("worker", 128, "Number of concurrent worker to read")

	runTime = flag.Duration("run-time", 3*time.Minute, "Actual workload runtime")

	warmUpTime = flag.Duration("warm-up-time", 2*time.Second, "Ramp up time")

	grpcConnPoolSize = flag.Int("grpc-conn-pool-size", 1, "grpc connection pool size")

	maxRetryDuration = 30 * time.Second

	retryMultiplier = 2.0

	bucketName = flag.String("bucket", "kislayk_europe_west4", "GCS bucket name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")

	totalDownload = flag.Int("total-mbytes", 0, "Stop after this amount of MiB downloaded.")

	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	memprofile = flag.String("memprofile", "", "write memory profile to `file`")

	// Object name = objectNamePrefix + {thread_id} + objectNameSuffix
	objectNamePrefix = flag.String("obj-prefix", "1GB/experiment.", "Object prefix")
	objectNameSuffix = flag.String("obj-suffix", ".0", "Object suffix")
)

// CreateHTTPClient create http storage client.
func CreateHTTPClient(ctx context.Context) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	transport = &http.Transport{
		MaxConnsPerHost:     maxConnsPerHost,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		// This disables HTTP/2 in transport.
		TLSNextProto: make(
			map[string]func(string, *tls.Conn) http.RoundTripper,
		),
	}

	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("while generating tokenSource, %v", err)
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &userAgentRoundTripper{
			wrapped: &oauth2.Transport{
				Base:   transport,
				Source: tokenSource,
			},
			UserAgent: "prince",
		},
		Timeout: 0,
	}
	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

func rampUp(warmupCtx context.Context, cancelFn context.CancelFunc, bucketHandle *storage.BucketHandle) {
	idx := 0
	var eG errgroup.Group
	for {
		select {
		case <-warmupCtx.Done():
			cancelFn()
			return
		default:
			if idx == *numOfWorkers {
				eG.Wait()
				return
			}
			time.Sleep(1 * time.Second)
			eG.Go(func() error {
				_, err := ReadObject(warmupCtx, idx, bucketHandle)
				if err != nil {
					err = fmt.Errorf("while reading object %v: %w", *objectNamePrefix+strconv.Itoa(idx)+*objectNameSuffix, err)
					return err
				}
				return err
			})
			idx++
		}
	}
}

// CreateGrpcClient creates grpc client.
func CreateGrpcClient(ctx context.Context, p *peer.Peer) (client *storage.Client, err error) {
	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, err
	}
	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(*grpcConnPoolSize), option.WithTokenSource(tokenSource), storage.WithDisabledClientMetrics(), option.WithGRPCDialOption(grpc.WithDefaultCallOptions(grpc.Peer(p))))
}

// ReadObject creates reader object corresponding to workerID with the help of bucketHandle.
func ReadObject(ctx context.Context, workerID int, bucketHandle *storage.BucketHandle) (bytesRead int64, err error) {
	objectName := *objectNamePrefix + strconv.Itoa(workerID) + *objectNameSuffix

	select {
	case <-ctx.Done():
		return 0, nil
	default:
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(ctx)
		if err != nil {
			return 0, fmt.Errorf("while creating reader object: %v", err)
		}
		defer rc.Close()

		// Calls Reader.WriteTo implicitly.
		count, err := io.Copy(io.Discard, rc)
		bytesRead += count
		if err != nil {
			return bytesRead, fmt.Errorf("while reading and discarding content: %v", err)
		}
		return bytesRead, nil
	}
}

func main() {
	flag.Parse()
	fmt.Printf("Starting benchmark with params:\n")
	fmt.Printf("workers: %d, grpcConnPoolSize: %d\n", *numOfWorkers, *grpcConnPoolSize)

	ctx, cancel := context.WithCancel(context.Background())

	fmt.Printf("Workload start time: %s\n", time.Now().String())

	var client *storage.Client
	var err error
	p := &peer.Peer{}
	protocol := ""
	if *clientProtocol == "http" {
		protocol = "http"
		client, err = CreateHTTPClient(ctx)
	} else {
		protocol = "grpc"
		client, err = CreateGrpcClient(ctx, p)
	}

	if err != nil {
		fmt.Printf("while creating the client: %v", err)
		os.Exit(1)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        maxRetryDuration,
			Multiplier: retryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	bucketHandle := client.Bucket(*bucketName)

	var totalBytesRead atomic.Int64

	warmupCtx, cancelFn := context.WithDeadline(ctx, time.Now().Add(*warmUpTime))
	//fmt.Println("Ramp-up starts")

	rampUp(warmupCtx, cancelFn, bucketHandle)

	// runtime.SetMutexProfileFraction(1)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	//fmt.Println("Ramp-up complete. Starting run on actual traffic.")
	startTime := time.Now()
	var eG errgroup.Group

	actualRunCtx, cancelFn := context.WithDeadline(ctx, startTime.Add(*runTime))
	defer cancelFn()

	var errCount atomic.Int64
	mu := sync.Mutex{}
	backneds := map[string]bool{}

	// Run the actual workload
	for i := range *numOfWorkers {
		idx := i
		eG.Go(func() error {
			//fmt.Printf("Worker %d started\n", idx)
			for {
				select {
				case <-actualRunCtx.Done():
					return nil
				default:
					bytesRead, err := ReadObject(ctx, idx, bucketHandle)
					if err != nil {
						errCount.Add(1)
						fmt.Println("Debug: ", err)
						continue
					}
					mu.Lock()
					addr := p.Addr
					if addr != nil {
						backneds[addr.String()] = true
					}
					mu.Unlock()
					totalBytesRead.Add(bytesRead)
					if *totalDownload > 0 && totalBytesRead.Load() > int64(*totalDownload)*MiB {
						return nil
					}
				}
			}
		})
	}

	err = eG.Wait()
	totalDuration := time.Since(startTime)
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}

	fmt.Println("Number of unique backends used: ", len(backneds))
	fmt.Printf("%v\n", backneds)

	cancel()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		// Lookup("allocs") creates a profile similar to go test -memprofile.
		// Alternatively, use Lookup("heap") for a profile
		// that has inuse_space as the default index.
		if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	// fmt.Println("MUTEX INFO START")
	// pprof.Lookup("mutex").WriteTo(os.Stdout, 0)
	// fmt.Println("MUTEX INFO END")

	if err == nil && err != context.DeadlineExceeded {
		bndwth := float64(1_000_000) / float64(MiB) * float64(totalBytesRead.Load()) / float64(totalDuration.Microseconds())

		fmt.Printf("Protocol: %s, Bandwidth: %.0f MiB/s, errors: %d\n", protocol, bndwth, errCount.Load())
		fmt.Printf("Workload end time: %s\n\n", time.Now().String())
		if *cpuprofile != "" {
			fmt.Println("Waiting to exit...")
			time.Sleep(5 * time.Minute)
		}
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
	fmt.Printf("Workload end time: %s\n\n", time.Now().String())
	os.Exit(1)
}
