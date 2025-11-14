package main

import (
	"testing"
)

func TestBuildBlobStoreLocal(t *testing.T) {
	root := t.TempDir()
	store, err := buildBlobStore("local", storageOptions{Root: root})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatalf("expected path store instance")
	}
}

func TestBuildBlobStoreS3Validation(t *testing.T) {
	if _, err := buildBlobStore("s3", storageOptions{}); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestBuildBlobStoreS3Success(t *testing.T) {
	store, err := buildBlobStore("s3", storageOptions{
		Endpoint:  "https://s3.example.com",
		Bucket:    "bucket",
		Region:    "us-east-1",
		AccessKey: "ak",
		SecretKey: "sk",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatalf("expected store instance")
	}
}
