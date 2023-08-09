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
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
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

	workerClient *worker.Client
	busClient    *bus.Client
	s3Client     *s3.Client
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

func objectExists(renterdPath string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	obj, _, err := busClient.Object(ctx, renterdPath, api.ObjectsWithLimit(1), api.ObjectsWithOffset(0))
	if err == nil {
		return true, nil
	} else if strings.Contains(err.Error(), "object not found") {
		return false, nil
	} else if strings.Contains(err.Error(), "no slabs found") {
		return false, nil
	} else if obj.Health <= 25 {
		return false, nil
	}
	return false, err
}

func uploadObject(bucket, key, renterdPath string) ([32]byte, error) {
	content, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}
	defer content.Body.Close()

	h := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(content.Body, h), 256*(1<<20)) // 256 MiB buffer
	if err := workerClient.UploadObject(context.Background(), r, renterdPath, api.UploadWithContractSet(contractSet), api.UploadWithRedundancy(minShards, totalShards)); err != nil {
		return [32]byte{}, fmt.Errorf("failed to upload %s: %w", renterdPath, err)
	}
	var checksum [32]byte
	copy(checksum[:], h.Sum(nil))
	return checksum, nil
}

func redundantSize(size uint64, minShards, totalShards int) uint64 {
	return uint64(math.Ceil(float64(size)/float64(uint64(minShards)*rhp2.SectorSize))) * uint64(totalShards) * rhp2.SectorSize
}

func main() {
	logCfg := zap.NewProductionConfig()
	logCfg.OutputPaths = []string{logPath, "stdout"}
	logCfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	log, err := logCfg.Build()
	if err != nil {
		panic(err)
	}

	busClient = bus.NewClient(busAddr, busPass)
	workerClient = worker.NewClient(workerAddr, workerPass)
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion), config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		log.Fatal("failed to load AWS config", zap.Error(err))
	}

	client := s3.NewFromConfig(cfg)
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

	for i := 0; i < threads; i++ {
		log := log.Named("upload").With(zap.Int("worker", i+1))
		go func(log *zap.Logger) {
			for obj := range uploadQueue {
				var uploadErr error
				renterdPath := filepath.Join(*bucket, *obj.Key)
				var checksum [32]byte
				log := log.With(zap.String("key", *obj.Key), zap.String("renterdPath", renterdPath), zap.Int64("bytes", obj.Size))
				log.Info("starting upload")
				var start time.Time
				for j := 0; j < 10; j++ { // retry failed uploads
					start = time.Now()
					checksum, uploadErr = uploadObject(*bucket, *obj.Key, renterdPath)
					if uploadErr != nil {
						log.Error("upload attempt failed", zap.Int("attempt", j+1), zap.Error(uploadErr), zap.Duration("elapsed", time.Since(start)))
						time.Sleep(30 * time.Second)
					} else {
						break
					}
				}
				elapsed := time.Since(start)
				if uploadErr != nil {
					log.Fatal("upload failed", zap.Error(uploadErr))
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

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: bucket,
	})
	log = log.Named("aws").With(zap.String("bucket", *bucket))
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Fatal("failed to list objects", zap.Error(err))
		}
		for _, obj := range page.Contents {
			renterdPath := filepath.Join(*bucket, *obj.Key)
			exists, err := objectExists(renterdPath)
			if err != nil {
				log.Fatal("failed to check object existence", zap.Error(err))
			} else if exists {
				// calculate the redundant size of the object
				redundantSize := redundantSize(uint64(obj.Size), minShards, totalShards)
				// increment the global counters
				bytes := atomic.AddUint64(&totalBytes, redundantSize)
				n := atomic.AddUint64(&uploadedObjects, 1)
				log.Info("skipping existing object", zap.String("renterdPath", renterdPath), zap.Uint64("objects", n), zap.Uint64("totalBytes", bytes))
				continue
			}
			uploadQueue <- obj
		}
	}
}
