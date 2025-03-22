package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Store struct {
	client     *s3.Client
	bucketName string
}

func NewS3(client *s3.Client, bucketName string) *S3Store {
	return &S3Store{
		client:     client,
		bucketName: bucketName,
	}
}

func (s *S3Store) PutObject(ctx context.Context, key string, data []byte) error {
	r := bytes.NewReader(data)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(s.bucketName),
		Key:               aws.String(key),
		Body:              r,
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return fmt.Errorf("failed to upload data to S3: %w", err)
	}

	return nil
}

func (s *S3Store) GetObject(ctx context.Context, key string, dataRange [2]uint64) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	}

	if dataRange[0] < dataRange[1] {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", dataRange[0], dataRange[1]))
	} else {
		return nil, fmt.Errorf("invalid data range: %v", dataRange)
	}

	resp, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error retrieving data from S3: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading data from S3 response: %w", err)
	}

	return data, nil
}
