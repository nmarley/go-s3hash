package app

import (
	"context"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// create an s3agent struct
type s3agent struct {
	client *s3.Client
	bucket string
}

// create a new s3agent
func NewS3Agent(bucket string) *s3agent {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	return &s3agent{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
	}
}

// ListAllBuckets lists all buckets in the s3agent's account. Test method to
// ensure the s3agent is working
func (s *s3agent) ListAllBuckets() ([]string, error) {
	var buckets []string

	result, err := s.client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		return buckets, err
	}

	for _, item := range result.Buckets {
		buckets = append(buckets, *item.Name)
	}

	return buckets, nil
}

// GetObjectBytes gets the bytes of an object from the s3agent's bucket
func (s *s3agent) GetObjectBytes(key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}

	result, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		return nil, err
	}

	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}
