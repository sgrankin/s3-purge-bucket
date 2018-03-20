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
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rcrowley/go-metrics"
	"github.com/sgrankin/s3-purge-bucket/s3util"
)

var (
	s3URLs        []string
	countDeleters = flag.Int("workers", 64, "count of concurrent deleter workers")
	region        = flag.String("region", "us-east-1", "AWS Region")
	dryrun        = flag.Bool("dryrun", false, "skip any destructive actions")

	client *s3util.S3

	statObjsQueued = metrics.NewRegisteredCounter("objs_queued", nil)
)

const (
	usageFmt = `
usage: %s [options] <url>...

Where:
  url: S3 URL in the form s3://bucket or s3://bucket/prefix.  Use multiple URLs (via shell expansion) to parallelize listing.

Options:
`
)

type deleteRequest struct {
	bucket  string
	objects []s3.ObjectIdentifier
}

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), strings.TrimSpace(usageFmt)+"\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	s3URLs = flag.Args()
	if len(s3URLs) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	client = s3util.MustNewClient(*region)
}

func main() {
	log.Printf("deleting all objects in paths %v", s3URLs)

	go metricsLogger(3 * time.Second)
	purgeBuckets(s3URLs)
	s3util.LogMetrics() // log final metrics
}

func metricsLogger(period time.Duration) {
	for _ = range time.Tick(period) {
		s3util.LogMetrics()
	}
}

func purgeBuckets(rawurls []string) {
	var listers, deleters sync.WaitGroup
	queue := make(chan *deleteRequest, *countDeleters)

	buckets := make(map[string]bool)
	for _, rawurl := range rawurls {
		bucket, path := splitS3URL(rawurl)
		buckets[bucket] = true

		listers.Add(1)
		go func(bucket string, prefix string) {
			defer listers.Done()
			lister(bucket, prefix, queue)
		}(bucket, path)
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

	if !*dryrun {
		for bucket := range buckets {
			log.Printf("removing bucket %s", bucket)
			client.MustDeleteBucket(bucket)
		}
	}
}

func lister(bucket, prefix string, queue chan<- *deleteRequest) {
	log.Printf("listing %s/%s", bucket, prefix)
	client.MustListObjectVersions(bucket, prefix, func(objects []s3.ObjectIdentifier) {
		statObjsQueued.Inc(int64(len(objects)))
		queue <- &deleteRequest{
			bucket:  bucket,
			objects: objects,
		}
	})
	log.Printf("finished listing %s/%s", bucket, prefix)
}

func deleter(in <-chan *deleteRequest) {
	for req := range in {
		statObjsQueued.Dec(int64(len(req.objects)))
		if !*dryrun {
			client.MustDeleteObjectVersions(req.bucket, req.objects)
		}
	}
}

func splitS3URL(rawurl string) (bucket, prefix string) {
	u, err := url.Parse(rawurl)
	if err != nil {
		log.Fatalf("error: can't parse URL '%s'", rawurl)
	}

	if u.Scheme != "s3" {
		log.Fatalf("error: URL scheme must be s3 in URL '%s'", u)
	}

	return u.Host, strings.TrimLeft(u.Path, "/")
}
