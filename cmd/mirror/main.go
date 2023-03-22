package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/siacentral/apisdkgo/sia"
	"go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/bus"
	stypes "go.sia.tech/siad/types"
)

const sectors = 5 * (1 << 40) / rhp.SectorSize

var (
	bucketName string
	awsRegion  string

	workerAddr, workerPass string
	busAddr, busPass       string

	minShards, totalShards int
	contractSet            string

	threads int
)

func formatByteString(b uint64) string {
	const units = "KMGTPE"
	const factor = 1024

	// short-circuit for < 1024 bytes
	if b < factor {
		return fmt.Sprintf("%v bytes", b)
	}

	var i = -1
	rem := float64(b)
	for ; rem >= factor; i++ {
		rem /= factor
	}
	return fmt.Sprintf("%.2f %ciB", rem, units[i])
}

func formatBpsString(b uint64, t time.Duration) string {
	const units = "KMGTPE"
	const factor = 1000

	time := t.Truncate(time.Second).Seconds()
	if time <= 0 {
		return "0.00 bps"
	}

	// calculate bps
	speed := float64(b*8) / time

	// short-circuit for < 1000 bits/s
	if speed < factor {
		return fmt.Sprintf("%.2f bps", speed)
	}

	var i = -1
	for ; speed >= factor; i++ {
		speed /= factor
	}
	return fmt.Sprintf("%.2f %cbps", speed, units[i])
}

func init() {
	flag.StringVar(&bucketName, "aws.bucket", "", "bucket to mirror")
	flag.StringVar(&awsRegion, "aws.region", "us-west-2", "aws region")
	flag.StringVar(&workerAddr, "worker.address", "http://localhost:9980/api/worker", "worker address")
	flag.StringVar(&workerPass, "worker.password", "password", "worker password")
	flag.StringVar(&contractSet, "worker.contractset", "autopilot", "contract set to use")
	flag.IntVar(&minShards, "worker.minshards", 10, "minimum shards per file")
	flag.IntVar(&totalShards, "worker.totalshards", 30, "total shards per file")
	flag.IntVar(&threads, "threads", 2, "number of threads to use")

	flag.StringVar(&busAddr, "bus.address", "http://localhost:9980/api/bus", "bus address")
	flag.StringVar(&busPass, "bus.password", "password", "bus password")
	flag.Parse()
}

func objectExists(bus *bus.Client, renterdPath string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, _, err := bus.Object(ctx, renterdPath)
	if err == nil {
		return true, nil
	} else if strings.Contains(err.Error(), "object not found") {
		return false, nil
	}
	return false, err
}

func uploadObject(client *s3.Client, bucket, key, renterdPath string) error {
	content, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}
	defer content.Body.Close()

	r := bufio.NewReaderSize(content.Body, 256*(1<<20)) // 256 MiB buffer
	if err := workerUploadObject(context.Background(), r, renterdPath, contractSet, minShards, totalShards); err != nil {
		return fmt.Errorf("failed to upload %s: %w", renterdPath, err)
	}
	return nil
}

func redundantSize(size uint64, minShards, totalShards int) uint64 {
	return uint64(math.Ceil(float64(size)/float64(uint64(minShards)*rhp.SectorSize))) * uint64(totalShards) * rhp.SectorSize
}

func updateHostAllowlist() error {
	sc := sia.NewClient()
	b := bus.NewClient(busAddr, busPass)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// get current host allowlist
	allowlist, err := b.HostAllowlist(ctx)
	if err != nil {
		return fmt.Errorf("failed to get host allowlist: %w", err)
	}

	currentHosts := make(map[types.PublicKey]bool)
	for _, host := range allowlist {
		currentHosts[host] = true
	}

	var goodHosts, badHosts []types.PublicKey
	// get good hosts
	filter := make(sia.HostFilter)
	filter.WithAcceptingContracts(true)
	filter.WithBenchmarked(true)
	filter.WithMinAge(4320)
	filter.WithMinUploadSpeed(2.5e7)
	filter.WithMaxContractPrice(stypes.SiacoinPrecision.Div64(2))
	filter.WithMaxUploadPrice(stypes.SiacoinPrecision.Mul64(100).Div64(1e12))
	filter.WithMaxDownloadPrice(stypes.SiacoinPrecision.Mul64(5000).Div64(1e12))

	for i := 0; ; i++ {
		hosts, err := sc.GetActiveHosts(filter, i, 500)
		if err != nil {
			return fmt.Errorf("failed to get hosts: %w", err)
		}
		if len(hosts) == 0 {
			break
		}
		for _, host := range hosts {
			var pub types.PublicKey
			if err := pub.UnmarshalText([]byte(host.PublicKey)); err != nil {
				return fmt.Errorf("failed to unmarshal public key: %w", err)
			}
			if currentHosts[pub] {
				delete(currentHosts, pub)
				continue
			}
			goodHosts = append(goodHosts, pub)
		}
	}

	for pub := range currentHosts {
		badHosts = append(badHosts, pub)
	}

	if len(goodHosts) == 0 && len(badHosts) == 0 {
		return nil
	} else if err := b.UpdateHostAllowlist(ctx, goodHosts, badHosts); err != nil {
		return fmt.Errorf("failed to update host allowlist: %w", err)
	}
	log.Printf("updated host allowlist: added %d good hosts, removed %d bad hosts", len(goodHosts), len(badHosts))
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	bus := bus.NewClient(busAddr, busPass)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion), config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		log.Fatalln(err)
	}

	client := s3.NewFromConfig(cfg)
	bucket := aws.String(bucketName)

	var t *time.Timer
	t = time.AfterFunc(0, func() {
		if err := updateHostAllowlist(); err != nil {
			log.Println("WARN: failed to update host allowlist:", err)
		}
		t.Reset(30 * time.Minute)
	})

	log.Println("starting upload...")

	uploadQueue := make(chan awstypes.Object, threads)

	var uploadedBytes, redundantBytes, uploadedObjects uint64
	for i := 0; i < threads; i++ {
		go func(worker int) {
			for obj := range uploadQueue {
				var uploadErr error
				renterdPath := filepath.Join(*bucket, *obj.Key)
				start := time.Now()
				log.Printf("worker %d: starting upload of %s %s (object %d - %s)", worker, renterdPath, formatByteString(uint64(obj.Size)), atomic.LoadUint64(&uploadedObjects), formatByteString(atomic.LoadUint64(&uploadedBytes)))
				for j := 0; j < 10; j++ { // retry failed uploads
					if uploadErr = uploadObject(client, *bucket, *obj.Key, renterdPath); uploadErr != nil {
						log.Printf("worker %d: upload attempt %v failed: %v", worker, j+1, uploadErr)
						time.Sleep(30 * time.Second)
					} else {
						break
					}
				}
				if uploadErr != nil {
					log.Fatalf("worker %d: upload of %s failed: %v", worker, renterdPath, uploadErr)
				}
				elapsed := time.Since(start)
				redundantSize := redundantSize(uint64(obj.Size), minShards, totalShards)
				u := atomic.AddUint64(&uploadedBytes, uint64(obj.Size))
				n := atomic.AddUint64(&uploadedObjects, 1)
				ru := atomic.AddUint64(&redundantBytes, redundantSize)
				log.Printf("finished upload of %s %s in %s (%s including redundandancy - %s) (object %d - %s - %s including redundancy)", renterdPath, formatByteString(uint64(obj.Size)), elapsed, formatByteString(redundantSize), formatBpsString(redundantSize, elapsed), n, formatByteString(u), formatByteString(ru))
			}
		}(i)
	}

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: bucket,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		for _, obj := range page.Contents {
			renterdPath := filepath.Join(*bucket, *obj.Key)
			exists, err := objectExists(bus, renterdPath)
			if err != nil {
				log.Fatalln(err)
			} else if exists {
				redundantSize := redundantSize(uint64(obj.Size), minShards, totalShards)
				uploaded := atomic.AddUint64(&uploadedBytes, uint64(obj.Size))
				ru := atomic.AddUint64(&redundantBytes, redundantSize)
				n := atomic.AddUint64(&uploadedObjects, 1)
				log.Printf("skipping existing object: %v (%d - %s - %s included redundancy)", renterdPath, n, formatByteString(uploaded), formatByteString(ru))
				continue
			}
			uploadQueue <- obj
		}
	}
}
