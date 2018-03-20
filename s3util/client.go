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

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rcrowley/go-metrics"
)

const (
	ErrCodeInternalError = "InternalError"
)

var (
	statClientRequests = metrics.NewRegisteredCounter("requests_total", nil)
)

type S3 struct {
	*s3.S3
}

func MustNewClient(region string) *S3 {
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		log.Fatalf("error: unable to configure AWS SDK: %v", err)
	}
	cfg.Region = region

	return &S3{s3.New(cfg)}
}
