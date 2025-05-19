package bucketdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Object interface {
	Name() string
	Index() []string
	ObjectID() string
}

type Client[O Object] struct {
	bucketName  string
	minioClient *minio.Client
	new         func() O
}

func NewClient[O Object](accessKeyID string, secretAccessKey string, endpoint string, bucketName string, useSSL bool, new func() O) (Client[O], error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return Client[O]{}, fmt.Errorf("failed to init minio client: %v", err)
	}
	return Client[O]{
		bucketName:  bucketName,
		minioClient: minioClient,
		new:         new,
	}, nil
}

func NewClientFromMinioClient[O Object](minioClient *minio.Client, bucketName string, new func() O) Client[O] {
	return Client[O]{
		bucketName:  bucketName,
		minioClient: minioClient,
		new:         new,
	}
}

func (c Client[O]) Create(ctx context.Context, obj O) error {
	objectBytes, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal the object: %v", err)
	}
	_, err = c.minioClient.PutObject(ctx, c.bucketName, c.objectKey(obj.ObjectID()), bytes.NewReader(objectBytes), int64(len(objectBytes)), minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return fmt.Errorf("failed to put the object into the bucket: %v", err)
	}
	for _, index := range obj.Index() {
		_, err = c.minioClient.PutObject(ctx, c.bucketName, index,
			bytes.NewReader([]byte{}), 0, minio.PutObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to put the index into the bucket: %v", err)
		}
	}
	return nil
}

func (c Client[O]) Get(ctx context.Context, id string) (O, error) {
	key := c.objectKey(id)
	reader, err := c.minioClient.GetObject(ctx, c.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return c.new(), fmt.Errorf("failed to get object %s: %v", key, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return c.new(), fmt.Errorf("failed to read object %s: %v", key, err)
	}
	obj := c.new()
	if err := json.Unmarshal(data, obj); err != nil {
		return c.new(), fmt.Errorf("failed to unmarshal object %s: %v", key, err)
	}
	return obj, nil
}

func (c Client[O]) List(ctx context.Context, index string, filter func(O) bool) ([]O, error) {
	opts := minio.ListObjectsOptions{
		Prefix:    c.indexKey(index),
		Recursive: true,
	}
	var results []O
	for object := range c.minioClient.ListObjects(ctx, c.bucketName, opts) {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing object: %v", object.Err)
		}
		_, id := filepath.Split(object.Key)
		obj, err := c.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get the object %s: %v", id, err)
		}
		if filter == nil || filter(obj) {
			results = append(results, obj)
		}
	}
	return results, nil
}

func (c Client[O]) Delete(ctx context.Context, id string) error {
	obj, err := c.Get(ctx, id)
	if err != nil {
		return err
	}
	for _, index := range c.indexKeys(obj) {
		if err := c.minioClient.RemoveObject(ctx, c.bucketName, index, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete index %s: %v", index, err)
		}
	}
	key := c.objectKey(id)
	if err := c.minioClient.RemoveObject(ctx, c.bucketName, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("failed to delete object %s: %v", key, err)
	}
	return nil
}

func (c Client[O]) Update(ctx context.Context, obj O) error {
	if err := c.Delete(ctx, obj.ObjectID()); err != nil {
		return fmt.Errorf("failed to delete old object for update: %v", err)
	}
	if err := c.Create(ctx, obj); err != nil {
		return fmt.Errorf("failed to create updated object: %v", err)
	}
	return nil
}

func (c Client[O]) objectKey(id string) string {
	return filepath.Join(c.new().Name(), "objects", id)
}

func (c Client[O]) indexKey(index string) string {
	return filepath.Join(c.new().Name(), "index", index)
}

func (c Client[O]) indexKeys(obj O) []string {
	src := obj.Index()
	res := make([]string, len(src))
	for i, s := range src {
		res[i] = c.indexKey(s)
	}
	return res
}
