package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/siacentral/apisdkgo/sia"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"lukechampine.com/frand"
)

var (
	logPath    string
	bucketName string
	awsRegion  string

	workerAddr, workerPass string
	busAddr, busPass       string

	minShards, totalShards int
	contractSet            string

	verifyUploads bool

	threads int
)

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
	flag.StringVar(&logPath, "log.path", "mirror.log", "log file path")
	flag.StringVar(&bucketName, "aws.bucket", "", "bucket to mirror")
	flag.StringVar(&awsRegion, "aws.region", "us-west-2", "aws region")
	flag.StringVar(&workerAddr, "worker.address", "http://localhost:9980/api/worker", "worker address")
	flag.StringVar(&workerPass, "worker.password", "password", "worker password")
	flag.StringVar(&contractSet, "worker.contractset", "autopilot", "contract set to use")
	flag.IntVar(&minShards, "worker.minshards", 10, "minimum shards per file")
	flag.IntVar(&totalShards, "worker.totalshards", 30, "total shards per file")
	flag.IntVar(&threads, "threads", 2, "number of threads to use")
	flag.BoolVar(&verifyUploads, "verify", false, "periodically verify uploaded data")

	flag.StringVar(&busAddr, "bus.address", "http://localhost:9980/api/bus", "bus address")
	flag.StringVar(&busPass, "bus.password", "password", "bus password")
	flag.Parse()
}

func uploadObject(client *s3.Client, workerClient *worker.Client, bucket, key string) ([32]byte, error) {
	content, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}
	defer content.Body.Close()

	h := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(content.Body, h), 256*(1<<20)) // 256 MiB buffer
	_, err = workerClient.UploadObject(context.Background(), r, bucket, key, api.UploadObjectOptions{})
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to upload %s: %w", key, err)
	}
	var checksum [32]byte
	copy(checksum[:], h.Sum(nil))
	return checksum, nil
}

func redundantSize(size uint64, minShards, totalShards int) uint64 {
	return uint64(math.Ceil(float64(size)/float64(uint64(minShards)*rhp2.SectorSize))) * uint64(totalShards) * rhp2.SectorSize
}

func updateHostAllowlist(ctx context.Context) (good int, bad int, _ error) {
	sc := sia.NewClient()
	b := bus.NewClient(busAddr, busPass)

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// get current host allowlist
	allowlist, err := b.HostAllowlist(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get host allowlist: %w", err)
	}

	currentHosts := make(map[types.PublicKey]bool)
	for _, host := range allowlist {
		currentHosts[host] = true
	}

	var goodHosts, badHosts []types.PublicKey
	// get top 500 fastest hosts
	for i := 0; i < 5; i++ {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		default:
		}

		hosts, err := sc.GetActiveHosts(i, 100,
			sia.HostFilterBenchmarked(true),
			sia.HostFilterSort(sia.HostSortDownloadSpeed, true))
		if err != nil {
			break
		}

		for _, host := range hosts {
			var pub types.PublicKey
			if err := pub.UnmarshalText([]byte(host.PublicKey)); err != nil {
				return 0, 0, fmt.Errorf("failed to unmarshal public key: %w", err)
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
		return 0, 0, nil
	} else if err := b.UpdateHostAllowlist(ctx, goodHosts, badHosts, false); err != nil {
		return 0, 0, fmt.Errorf("failed to update host allowlist: %w", err)
	}
	return len(goodHosts), len(badHosts), nil
}

func main() {
	logCfg := zap.NewProductionConfig()
	logCfg.OutputPaths = []string{logPath, "stdout"}
	logCfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	log, err := logCfg.Build()
	if err != nil {
		panic(err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion), config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		log.Fatal("failed to load AWS config", zap.Error(err))
	}

	s3Client := s3.NewFromConfig(cfg)
	bucket := aws.String(bucketName)

	log.Info("starting mirror")
	uploadQueue := make(chan awstypes.Object, threads)
	var uploadedBytes, redundantBytes, totalBytes, uploadedObjects uint64
	uploadStart := time.Now()

	s := rate.Sometimes{Interval: time.Minute}
	logProgress := func() {
		s.Do(func() {
			elapsed := time.Since(uploadStart)
			uploadedBytes := atomic.LoadUint64(&redundantBytes)
			totalBytes := atomic.LoadUint64(&totalBytes)
			n := atomic.LoadUint64(&uploadedObjects)
			log.Info("upload progress", zap.Uint64("bytes", atomic.LoadUint64(&uploadedBytes)), zap.String("speed", formatBpsString(uploadedBytes, elapsed)), zap.Uint64("redundantBytes", uploadedBytes), zap.Uint64("objects", n), zap.Uint64("totalBytes", totalBytes), zap.Duration("elapsed", elapsed))
		})
	}

	/*go func() {
		for {
			added, removed, err := updateHostAllowlist(context.Background())
			if err != nil {
				log.Error("failed to update host allowlist", zap.Error(err))
			}
			log.Info("updated host allowlist", zap.Int("added", added), zap.Int("removed", removed))
			time.Sleep(5 * time.Minute)
		}
	}()*/

	workerClient := worker.NewClient(workerAddr, workerPass)

	for i := 0; i < threads; i++ {
		log := log.Named("upload").With(zap.Int("worker", i+1))
		go func(log *zap.Logger) {
			for obj := range uploadQueue {
				var uploadErr error
				var checksum [32]byte
				log := log.With(zap.String("key", *obj.Key), zap.Int64("bytes", obj.Size))
				log.Info("starting upload")
				var start time.Time
				for j := 0; ; j++ { // retry failed uploads
					start = time.Now()
					checksum, uploadErr = uploadObject(s3Client, workerClient, *bucket, *obj.Key)
					if uploadErr != nil {
						log.Error("upload attempt failed", zap.Int("attempt", j+1), zap.Error(uploadErr), zap.Duration("elapsed", time.Since(start)))
						sleepTime := time.Duration(math.Pow(2+frand.Float64(), float64(j))) * time.Millisecond
						if sleepTime > time.Minute {
							sleepTime = time.Minute
						}
						time.Sleep(sleepTime)
					} else {
						break
					}
				}
				elapsed := time.Since(start)
				if uploadErr != nil {
					log.Error("upload failed", zap.Error(uploadErr))
				}
				// calculate the redundant size of the object
				redundantSize := redundantSize(uint64(obj.Size), minShards, totalShards)
				// increment the global counters
				atomic.AddUint64(&uploadedBytes, uint64(obj.Size))
				atomic.AddUint64(&uploadedObjects, 1)
				atomic.AddUint64(&totalBytes, redundantSize)
				atomic.AddUint64(&redundantBytes, redundantSize)
				log.Info("upload complete", zap.Uint64("redundantBytes", redundantSize), zap.Duration("elapsed", elapsed), zap.String("speed", formatBpsString(redundantSize, elapsed)), zap.String("checksum", hex.EncodeToString(checksum[:])))
				logProgress()
			}
		}(log)
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: bucket,
	})
	log = log.Named("aws").With(zap.String("bucket", *bucket))
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Fatal("failed to list objects", zap.Error(err))
		}
		for _, obj := range page.Contents {
			uploadQueue <- obj
		}
	}
}
