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

import "flag"
import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	region     = flag.String("region", "us-east-1", "AWS Region")
	bucket     = flag.String("bucket", "", "the bucket")
	prefix     = flag.String("prefix", "", "S3 prefix in bucket")
	numWorkers = flag.Int("workers", 16, "number of concurrent deleter workers")

	client *s3.S3

	stat struct {
		listed          int64
		requests        int64
		queued          int64
		deletesPending  int64
		deleted         int64
		keyMarker       string
		versionIdMarker string
	}

	wg sync.WaitGroup
)

func init() {
	flag.Parse()

	if *bucket == "" {
		flag.Usage()
		os.Exit(1)
	}

	client = s3.New(session.New(), aws.NewConfig().WithRegion(*region))
}

func lister(out chan<- []*s3.ObjectIdentifier) {
	prefixOrNil := prefix
	if *prefixOrNil == "" {
		prefixOrNil = nil
	}

	err := client.ListObjectVersionsPages(&s3.ListObjectVersionsInput{
		Bucket: bucket,
		Prefix: prefixOrNil,
	}, func(p *s3.ListObjectVersionsOutput, lastPage bool) (shouldContinue bool) {
		atomic.AddInt64(&stat.requests, 1)
		result := make([]*s3.ObjectIdentifier, 0, len(p.Versions)+len(p.DeleteMarkers))
		for _, ver := range p.Versions {
			result = append(result, &s3.ObjectIdentifier{
				Key:       ver.Key,
				VersionId: ver.VersionId,
			})
		}
		for _, ver := range p.DeleteMarkers {
			result = append(result, &s3.ObjectIdentifier{
				Key:       ver.Key,
				VersionId: ver.VersionId,
			})
		}

		atomic.AddInt64(&stat.listed, int64(len(result)))
		if p.NextKeyMarker != nil {
			stat.keyMarker = *p.NextKeyMarker
		} else {
			stat.keyMarker = ""
		}
		if p.VersionIdMarker != nil {
			stat.versionIdMarker = *p.VersionIdMarker
		} else {
			stat.versionIdMarker = ""
		}

		if len(result) > 0 {
			atomic.AddInt64(&stat.queued, int64(len(result)))
			out <- result
		}
		shouldContinue = true
		return
	})
	if err != nil {
		log.Fatalf("error while listing: %v", err)
	}
	close(out)
	wg.Done()
}

func deleter(in <-chan []*s3.ObjectIdentifier) {
	for objs := range in {
		atomic.AddInt64(&stat.queued, -int64(len(objs)))
		atomic.AddInt64(&stat.deletesPending, 1)

		out, err := client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: bucket,
			Delete: &s3.Delete{
				Objects: objs,
			},
		})
		atomic.AddInt64(&stat.requests, 1)
		atomic.AddInt64(&stat.deletesPending, -1)
		if err != nil {
			log.Fatalf("error while deleting: %v", err)
		}
		if len(out.Errors) > 0 {
			log.Fatalf("errors while deleting: %v", out.Errors)
		}
		atomic.AddInt64(&stat.deleted, int64(len(out.Deleted)))
	}
	wg.Done()
}

func progress() {
	for {
		time.Sleep(time.Second)
		requests := atomic.LoadInt64(&stat.requests)
		listed := atomic.LoadInt64(&stat.listed)
		queued := atomic.LoadInt64(&stat.queued)
		deleted := atomic.LoadInt64(&stat.deleted)
		deletesPending := atomic.LoadInt64(&stat.deletesPending)

		log.Printf("requests:%d listed:%d queued:%d deletesPending:%d deleted:%d key:%s versionId:%s",
			requests, listed, queued, deletesPending, deleted,
			stat.keyMarker, stat.versionIdMarker,
		)
	}
}

func deleteBucket() {
	_, err := client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: bucket,
	})
	if err != nil {
		log.Fatalf("error while deleting bucket: %v", err)
	}
}

func main() {
	log.Printf("deleting all objects in bucket %v", *bucket)
	go progress()
	wg.Add(1 + *numWorkers)
	wq := make(chan []*s3.ObjectIdentifier, *numWorkers*2)
	go lister(wq)
	for i := 0; i < *numWorkers; i++ {
		go deleter(wq)
	}
	wg.Wait()

	if *prefix == "" {
		log.Printf("deleting bucket")
		deleteBucket()
	}
}
