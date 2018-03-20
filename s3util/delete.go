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

package s3util

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rcrowley/go-metrics"
)

var (
	statDeletesPending = metrics.NewRegisteredCounter("deletes_pending", nil)
	statObjsDeleted    = metrics.NewRegisteredCounter("objs_deleted_total", nil)
)

func (client *S3) DeleteBucket(bucket string) error {
	log.Printf("removing bucket %s", bucket)
	_, err := client.S3.DeleteBucketRequest(&s3.DeleteBucketInput{
		Bucket: &bucket,
	}).Send()
	statClientRequests.Inc(1)
	return err
}

func (client *S3) MustDeleteBucket(bucket string) {
	if err := client.DeleteBucket(bucket); err != nil {
		log.Fatalf("error while deleting bucket %s: %v", bucket, err)
	}
}

func (client *S3) DeleteObjectVersions(bucket string, objects []s3.ObjectIdentifier) error {
	statDeletesPending.Inc(1)
	out, err := client.DeleteObjectsRequest(&s3.DeleteObjectsInput{
		Bucket: &bucket,
		Delete: &s3.Delete{
			Objects: objects,
		},
	}).Send()
	statClientRequests.Inc(1)
	statDeletesPending.Dec(1)

	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == ErrCodeInternalError {
			return client.DeleteObjectVersions(bucket, objects)
		}
		log.Fatalf("error while deleting: %v", err)
	}

	statObjsDeleted.Inc(int64(len(out.Deleted)))

	if len(out.Errors) > 0 {
		retryableObjects := make([]s3.ObjectIdentifier, 0)
		for _, err := range out.Errors {
			if *err.Code == ErrCodeInternalError {
				retryableObjects = append(retryableObjects, s3.ObjectIdentifier{
					Key:       err.Key,
					VersionId: err.VersionId,
				})
			}
		}

		if len(retryableObjects) == len(out.Errors) { // all failures are retryable
			return client.DeleteObjectVersions(bucket, retryableObjects)
		}
		log.Fatalf("non-retryable errors while deleting: %v", out.Errors)
	}

	return err
}
func (client *S3) MustDeleteObjectVersions(bucket string, objects []s3.ObjectIdentifier) {
	err := client.DeleteObjectVersions(bucket, objects)
	if err != nil {
		log.Fatalf("error while deleting: %v", err)
	}
}
