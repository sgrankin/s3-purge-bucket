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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rcrowley/go-metrics"
)

var (
	statObjsListed = metrics.NewRegisteredCounter("objs_listed_total", nil)
)

func (client *S3) ListObjectVersions(
	bucket string, prefix string,
	out func(objects []s3.ObjectIdentifier),
) error {
	req := client.ListObjectVersionsRequest(&s3.ListObjectVersionsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	pager := req.Paginate()
	for pager.Next() {
		statClientRequests.Inc(1)

		page := pager.CurrentPage()
		objects := make([]s3.ObjectIdentifier, 0, len(page.Versions)+len(page.DeleteMarkers))
		for _, ver := range page.Versions {
			objects = append(objects, s3.ObjectIdentifier{
				Key:       ver.Key,
				VersionId: ver.VersionId,
			})
		}
		for _, ver := range page.DeleteMarkers {
			objects = append(objects, s3.ObjectIdentifier{
				Key:       ver.Key,
				VersionId: ver.VersionId,
			})
		}

		statObjsListed.Inc(int64(len(objects)))

		if len(objects) > 0 {
			out(objects)
		}
	}

	return pager.Err()
}

func (client *S3) MustListObjectVersions(
	bucket string, prefix string,
	out func(objects []s3.ObjectIdentifier),
) {
	if err := client.ListObjectVersions(bucket, prefix, out); err != nil {
		log.Fatalf("error while listing %s/%s: %v", bucket, prefix, err)
	}
}
