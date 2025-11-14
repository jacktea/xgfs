package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// Method enumerates supported encryption algorithms.
type Method string

const (
	// MethodNone skips encryption entirely.
	MethodNone Method = "none"
	// MethodAES256CTR encrypts data using AES-256 in CTR mode with a random IV prefix.
	MethodAES256CTR Method = "aes-256-ctr"
)

// Options describes how to encrypt or decrypt payloads.
type Options struct {
	Method Method
	Key    []byte
}

// Enabled reports whether encryption should run.
func (o Options) Enabled() bool {
	return o.Method != "" && o.Method != MethodNone
}

// Validate ensures the configuration is usable for the selected method.
func (o Options) Validate() error {
	if !o.Enabled() {
		return nil
	}
	switch o.Method {
	case MethodAES256CTR:
		if len(o.Key) != 32 {
			return fmt.Errorf("encryption: aes-256-ctr requires 32-byte key, got %d", len(o.Key))
		}
	default:
		return fmt.Errorf("encryption: unsupported method %q", o.Method)
	}
	return nil
}

// WrapWriter returns an io.Writer that encrypts streaming data before writing to dst.
// The returned int64 is the number of bytes already written to dst (e.g. IV headers).
func WrapWriter(dst io.Writer, opts Options) (io.Writer, int64, error) {
	if err := opts.Validate(); err != nil {
		return nil, 0, err
	}
	if !opts.Enabled() {
		return dst, 0, nil
	}
	switch opts.Method {
	case MethodAES256CTR:
		return wrapAES256CTRWriter(dst, opts.Key)
	default:
		return nil, 0, fmt.Errorf("encryption: unsupported method %q", opts.Method)
	}
}

// Encrypt returns data encrypted according to opts. The returned slice includes any IV header.
func Encrypt(data []byte, opts Options) ([]byte, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if !opts.Enabled() {
		return data, nil
	}
	switch opts.Method {
	case MethodAES256CTR:
		return encryptAES256CTR(data, opts.Key)
	default:
		return nil, fmt.Errorf("encryption: unsupported method %q", opts.Method)
	}
}

// Decrypt reverses Encrypt using opts.
func Decrypt(ciphertext []byte, opts Options) ([]byte, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if !opts.Enabled() {
		return ciphertext, nil
	}
	switch opts.Method {
	case MethodAES256CTR:
		return decryptAES256CTR(ciphertext, opts.Key)
	default:
		return nil, fmt.Errorf("encryption: unsupported method %q", opts.Method)
	}
}

// Overhead returns the number of bytes added by the given method (for IVs, etc).
func Overhead(method Method) int {
	switch method {
	case MethodAES256CTR:
		return aes.BlockSize
	default:
		return 0
	}
}

func wrapAES256CTRWriter(dst io.Writer, key []byte) (io.Writer, int64, error) {
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, 0, err
	}
	if _, err := dst.Write(iv); err != nil {
		return nil, 0, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, 0, err
	}
	stream := cipher.NewCTR(block, iv)
	return &cipher.StreamWriter{S: stream, W: dst}, int64(len(iv)), nil
}

func encryptAES256CTR(data, key []byte) ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, iv)
	payload := make([]byte, len(data))
	stream.XORKeyStream(payload, data)
	out := make([]byte, len(iv)+len(payload))
	copy(out, iv)
	copy(out[len(iv):], payload)
	return out, nil
}

func decryptAES256CTR(data, key []byte) ([]byte, error) {
	if len(data) < aes.BlockSize {
		return nil, errors.New("encryption: ciphertext missing IV")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	iv := data[:aes.BlockSize]
	payload := make([]byte, len(data)-aes.BlockSize)
	copy(payload, data[aes.BlockSize:])
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(payload, payload)
	return payload, nil
}
