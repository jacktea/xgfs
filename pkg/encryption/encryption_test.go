package encryption

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	opts := Options{
		Method: MethodAES256CTR,
		Key:    bytes.Repeat([]byte{0x42}, 32),
	}
	ciphertext, err := Encrypt([]byte("top-secret"), opts)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(ciphertext) <= len("top-secret") {
		t.Fatalf("ciphertext should include IV header")
	}
	plaintext, err := Decrypt(ciphertext, opts)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(plaintext) != "top-secret" {
		t.Fatalf("unexpected plaintext %q", plaintext)
	}
}

func TestWrapWriterEncryptsStream(t *testing.T) {
	opts := Options{
		Method: MethodAES256CTR,
		Key:    bytes.Repeat([]byte{0x7f}, 32),
	}
	var buf bytes.Buffer
	writer, overhead, err := WrapWriter(&buf, opts)
	if err != nil {
		t.Fatalf("wrap writer: %v", err)
	}
	if overhead != int64(Overhead(opts.Method)) {
		t.Fatalf("expected overhead %d, got %d", Overhead(opts.Method), overhead)
	}
	payload := []byte("stream-data")
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	ciphertext := buf.Bytes()
	if len(ciphertext) != len(payload)+Overhead(opts.Method) {
		t.Fatalf("expected ciphertext len %d, got %d", len(payload)+Overhead(opts.Method), len(ciphertext))
	}
	plaintext, err := Decrypt(ciphertext, opts)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(plaintext, payload) {
		t.Fatalf("round trip mismatch, got %q", plaintext)
	}
}

func TestValidateRejectsBadKey(t *testing.T) {
	err := (Options{Method: MethodAES256CTR, Key: []byte("short")}).Validate()
	if err == nil {
		t.Fatalf("expected validation failure for short key")
	}
}
