// Copyright 2018 Sergey Grankin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kujtimiihoxha/go-brace-expansion"
	"github.com/rcrowley/go-metrics"
)

const (
	internalErrorCode = "InternalError"
)

var (
	buckets       []string
	countDeleters = flag.Int("workers", 64, "count of concurrent deleter workers")
	prefixGlob    = flag.String("prefix", "", "list only this prefix; supports brace expansion for parallelization")
	region        = flag.String("region", "us-east-1", "AWS Region")

	client *s3.S3

	stat = struct {
		listed         metrics.Counter
		requests       metrics.Counter
		queued         metrics.Counter
		deletesPending metrics.Counter
		deleted        metrics.Counter
	}{
		listed:         metrics.NewRegisteredCounter("listed", nil),
		requests:       metrics.NewRegisteredCounter("requests", nil),
		queued:         metrics.NewRegisteredCounter("queued", nil),
		deletesPending: metrics.NewRegisteredCounter("deletesPending", nil),
		deleted:        metrics.NewRegisteredCounter("deleted", nil),
	}
)

type deleteRequest struct {
	bucket  string
	objects []*s3.ObjectIdentifier
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(
			flag.CommandLine.Output(), "usage: %s [OPTION]... [BUCKET]...\n\nOptions:\n",
			os.Args[0],
		)
		flag.PrintDefaults()
	}
	flag.Parse()
	buckets = flag.Args()

	if len(buckets) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	client = s3.New(session.New(), aws.NewConfig().WithRegion(*region))
}

func lister(bucket string, prefix string, out chan<- *deleteRequest) {
	target := fmt.Sprintf("%s/%s", bucket, prefix)
	log.Printf("listing %s", target)

	err := client.ListObjectVersionsPages(&s3.ListObjectVersionsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	}, func(p *s3.ListObjectVersionsOutput, lastPage bool) (shouldContinue bool) {
		stat.requests.Inc(1)
		objects := make([]*s3.ObjectIdentifier, 0, len(p.Versions)+len(p.DeleteMarkers))
		for _, ver := range p.Versions {
			objects = append(objects, &s3.ObjectIdentifier{
				Key:       ver.Key,
				VersionId: ver.VersionId,
			})
		}
		for _, ver := range p.DeleteMarkers {
			objects = append(objects, &s3.ObjectIdentifier{
				Key:       ver.Key,
				VersionId: ver.VersionId,
			})
		}

		stat.listed.Inc(int64(len(objects)))

		if len(objects) > 0 {
			stat.queued.Inc(int64(len(objects)))
			out <- &deleteRequest{
				bucket:  bucket,
				objects: objects,
			}
		}
		shouldContinue = true
		return
	})

	if err != nil {
		log.Fatalf("error while listing %s: %v", target, err)
	}
	log.Printf("finished listing %s", target)
}

func deleteObjects(bucket string, objects []*s3.ObjectIdentifier) error {
	stat.deletesPending.Inc(1)
	out, err := client.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &s3.Delete{
			Objects: objects,
		},
	})
	stat.requests.Inc(1)
	stat.deletesPending.Dec(1)

	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == internalErrorCode {
			return deleteObjects(bucket, objects)
		}
		log.Fatalf("error while deleting: %v", err)
	}

	stat.deleted.Inc(int64(len(out.Deleted)))

	if len(out.Errors) > 0 {
		retryableObjects := make([]*s3.ObjectIdentifier, 0)
		for _, err := range out.Errors {
			if *err.Code == internalErrorCode {
				retryableObjects = append(retryableObjects, &s3.ObjectIdentifier{
					Key:       err.Key,
					VersionId: err.VersionId,
				})
			}
		}

		if len(retryableObjects) == len(out.Errors) { // all failures are retryable
			return deleteObjects(bucket, retryableObjects)
		}
		log.Fatalf("non-retryable errors while deleting: %v", out.Errors)
	}

	return err
}

func deleter(in <-chan *deleteRequest) {
	for req := range in {
		stat.queued.Dec(int64(len(req.objects)))
		err := deleteObjects(req.bucket, req.objects)
		if err != nil {
			log.Fatalf("error while deleting: %v", err)
		}
	}
}

func deleteBucket(bucket string) {
	log.Printf("removing bucket %s", bucket)
	_, err := client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	stat.requests.Inc(1)
	if err != nil {
		log.Fatalf("error while deleting bucket %s: %v", bucket, err)
	}
}

func purgeBuckets(buckets []string) {
	queue := make(chan *deleteRequest, *countDeleters)

	var listers, deleters sync.WaitGroup
	prefixes := gobrex.Expand(*prefixGlob)
	if len(prefixes) == 0 {
		prefixes = []string{""}
	}

	for _, bucket := range buckets {
		for _, prefix := range prefixes {
			listers.Add(1)
			go func(bucket string, prefix string) {
				defer listers.Done()
				lister(bucket, prefix, queue)
			}(bucket, prefix)
		}
	}

	for i := 0; i < *countDeleters; i++ {
		deleters.Add(1)
		go func() {
			defer deleters.Done()
			deleter(queue)
		}()
	}
	listers.Wait()
	close(queue)
	deleters.Wait()

	for _, bucket := range buckets {
		deleteBucket(bucket)
	}
}

func logMetrics() {
	registry := metrics.DefaultRegistry

	keys := make([]string, 0)
	values := make(map[string]string)

	registry.Each(func(name string, i interface{}) {
		keys = append(keys, name)
		switch metric := i.(type) {
		case metrics.Counter:
			values[name] = strconv.FormatInt(metric.Count(), 10)
		case metrics.Gauge:
			values[name] = strconv.FormatInt(metric.Value(), 10)
		default:
			log.Fatalf("unknown metric type %v", metric)
		}
	})

	var buffer bytes.Buffer
	buffer.WriteString("metrics:")

	sort.Strings(keys)
	for _, k := range keys {
		buffer.WriteString(fmt.Sprintf(" %s:%s", k, values[k]))
	}

	log.Print(buffer.String())
}

func metricsLogger(period time.Duration) {
	for _ = range time.Tick(period) {
		logMetrics()
	}
}

func main() {
	log.Printf("deleting all objects in buckets %v", buckets)
	go metricsLogger(3 * time.Second)
	purgeBuckets(buckets)
	logMetrics()
	log.Printf("done")
}
