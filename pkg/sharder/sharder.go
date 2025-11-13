package sharder

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/jacktea/xgfs/pkg/blob"
)

// Shard represents a single encrypted chunk stored in blob storage.
type Shard struct {
	ID        blob.ID
	Size      int64
	Checksum  string
	Encrypted bool
}

// WriterOptions controls chunking behaviour.
type WriterOptions struct {
	ChunkSize   int64
	Encrypt     bool
	Key         []byte
	Concurrency int
}

// ChunkAndStore splits the reader into shards and persists them.
func ChunkAndStore(ctx context.Context, store blob.Store, r io.Reader, opts WriterOptions) ([]Shard, error) {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 4 << 20 // 4MiB default
	}
	if opts.Concurrency <= 1 {
		return chunkSequential(ctx, store, r, opts)
	}
	return chunkConcurrent(ctx, store, r, opts)
}

// ReaderOptions controls concat behavior.
type ReaderOptions struct {
	Key         []byte
	Concurrency int
}

// Concat reads shards and writes them sequentially into w using at most opts.Concurrency goroutines.
func Concat(ctx context.Context, store blob.Store, shards []Shard, w io.WriterAt, opts ReaderOptions) (int64, error) {
	if opts.Concurrency <= 1 {
		return concatSequential(ctx, store, shards, w, opts)
	}
	return concatConcurrent(ctx, store, shards, w, opts)
}

// --- internal helpers ---

func chunkSequential(ctx context.Context, store blob.Store, r io.Reader, opts WriterOptions) ([]Shard, error) {
	buf := make([]byte, opts.ChunkSize)
	var shards []Shard
	for {
		n, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n == 0 {
			break
		}
		chunk := append([]byte(nil), buf[:n]...)
		shard, err := persistChunk(ctx, store, chunk, opts)
		if err != nil {
			return nil, err
		}
		shards = append(shards, shard)
		if err == io.ErrUnexpectedEOF {
			break
		}
	}
	return shards, nil
}

func chunkConcurrent(ctx context.Context, store blob.Store, r io.Reader, opts WriterOptions) ([]Shard, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type chunkTask struct {
		index int
		data  []byte
	}
	type shardResult struct {
		index int
		shard Shard
		err   error
	}

	chunkCh := make(chan chunkTask)
	resultCh := make(chan shardResult, opts.Concurrency*2)
	var workerWG sync.WaitGroup

	worker := func() {
		defer workerWG.Done()
		for task := range chunkCh {
			shard, err := persistChunk(ctx, store, task.data, opts)
			resultCh <- shardResult{index: task.index, shard: shard, err: err}
			if err != nil {
				cancel()
				return
			}
		}
	}
	for i := 0; i < opts.Concurrency; i++ {
		workerWG.Add(1)
		go worker()
	}

	count := 0
	buf := make([]byte, opts.ChunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			cancel()
			return nil, err
		}
		if n == 0 {
			break
		}
		data := append([]byte(nil), buf[:n]...)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case chunkCh <- chunkTask{index: count, data: data}:
			count++
		}
		if err == io.ErrUnexpectedEOF {
			break
		}
	}
	close(chunkCh)

	go func() {
		workerWG.Wait()
		close(resultCh)
	}()

	shardsMap := make(map[int]Shard, count)
	for res := range resultCh {
		if res.err != nil {
			return nil, res.err
		}
		shardsMap[res.index] = res.shard
	}
	shards := make([]Shard, count)
	for i := 0; i < count; i++ {
		shard, ok := shardsMap[i]
		if !ok {
			return nil, fmt.Errorf("missing shard %d", i)
		}
		shards[i] = shard
	}
	return shards, nil
}

func persistChunk(ctx context.Context, store blob.Store, chunk []byte, opts WriterOptions) (Shard, error) {
	sum := sha256.Sum256(chunk)
	reader := bytes.NewReader(chunk)
	id, written, err := store.Put(ctx, reader, int64(len(chunk)), blob.PutOptions{
		Encrypt:  opts.Encrypt,
		Key:      opts.Key,
		Checksum: hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return Shard{}, err
	}
	return Shard{
		ID:        id,
		Size:      written,
		Checksum:  hex.EncodeToString(sum[:]),
		Encrypted: opts.Encrypt,
	}, nil
}

func concatSequential(ctx context.Context, store blob.Store, shards []Shard, w io.WriterAt, opts ReaderOptions) (int64, error) {
	var offset int64
	for _, shard := range shards {
		data, err := fetchShard(ctx, store, shard, opts.Key)
		if err != nil {
			return offset, err
		}
		if _, err := w.WriteAt(data, offset); err != nil {
			return offset, err
		}
		offset += int64(len(data))
	}
	return offset, nil
}

func concatConcurrent(ctx context.Context, store blob.Store, shards []Shard, w io.WriterAt, opts ReaderOptions) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type job struct {
		index int
		shard Shard
	}
	type result struct {
		index int
		data  []byte
		err   error
	}
	jobCh := make(chan job)
	resCh := make(chan result, opts.Concurrency*2)
	var workerWG sync.WaitGroup
	worker := func() {
		defer workerWG.Done()
		for j := range jobCh {
			data, err := fetchShard(ctx, store, j.shard, opts.Key)
			resCh <- result{index: j.index, data: data, err: err}
			if err != nil {
				cancel()
				return
			}
		}
	}
	for i := 0; i < opts.Concurrency; i++ {
		workerWG.Add(1)
		go worker()
	}

	go func() {
		defer func() {
			workerWG.Wait()
			close(resCh)
		}()
		for idx, shard := range shards {
			select {
			case <-ctx.Done():
				close(jobCh)
				return
			case jobCh <- job{index: idx, shard: shard}:
			}
		}
		close(jobCh)
	}()
	pending := make(map[int][]byte)
	next := 0
	var offset int64
	for res := range resCh {
		if res.err != nil {
			return offset, res.err
		}
		pending[res.index] = res.data
		for {
			data, ok := pending[next]
			if !ok {
				break
			}
			if _, err := w.WriteAt(data, offset); err != nil {
				cancel()
				return offset, err
			}
			offset += int64(len(data))
			delete(pending, next)
			next++
		}
	}
	return offset, nil
}

func fetchShard(ctx context.Context, store blob.Store, shard Shard, key []byte) ([]byte, error) {
	rc, _, err := store.Get(ctx, shard.ID)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if shard.Encrypted {
		if len(key) != 32 {
			return nil, errors.New("concat: missing 32-byte key for encrypted shard")
		}
		if len(data) < aes.BlockSize {
			return nil, errors.New("concat: encrypted shard missing IV")
		}
		iv := data[:aes.BlockSize]
		payload := append([]byte(nil), data[aes.BlockSize:]...)
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		stream := cipher.NewCTR(block, iv)
		stream.XORKeyStream(payload, payload)
		return payload, nil
	}
	return data, nil
}
