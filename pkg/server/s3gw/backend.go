package s3gw

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/johannesboyne/gofakes3"

	"github.com/jacktea/xgfs/pkg/fs"
	"github.com/jacktea/xgfs/pkg/vfs"
)

const (
	etagXAttr       = "user.xgfs.s3.etag"
	uploadsDirName  = ".s3uploads"
	uploadMetaFile  = "meta.json"
	partFilePattern = "part-%05d"
)

// Backend implements gofakes3.Backend + MultipartBackend on top of a Filesystem.
type Backend struct {
	fs        Filesystem
	uploadSeq uint64
}

var (
	_ gofakes3.Backend          = (*Backend)(nil)
	_ gofakes3.MultipartBackend = (*Backend)(nil)
)

// NewBackend wraps Filesystem with an S3-compatible backend.
func NewBackend(fs Filesystem) *Backend {
	return &Backend{fs: fs}
}

func (b *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	ctx := context.Background()
	ch, err := b.fs.List(ctx, "/", fs.ListOptions{})
	if err != nil && !errors.Is(err, fs.ErrNotFound) {
		return nil, err
	}
	var buckets []gofakes3.BucketInfo
	for entry := range ch {
		if entry.Dir == nil {
			continue
		}
		name := strings.TrimPrefix(entry.Dir.Path(), "/")
		if name == "" || isReservedName(name) {
			continue
		}
		meta := entry.Dir.Metadata()
		ts := meta.MTime
		if ts.IsZero() {
			ts = time.Now()
		}
		buckets = append(buckets, gofakes3.BucketInfo{
			Name:         name,
			CreationDate: gofakes3.NewContentTime(ts),
		})
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	return buckets, nil
}

func (b *Backend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, name); err != nil {
		return nil, err
	}
	if prefix == nil {
		prefix = &gofakes3.Prefix{}
	}
	objects, err := b.listObjects(ctx, name)
	if err != nil {
		return nil, err
	}
	limit := int(page.MaxKeys)
	if limit <= 0 {
		limit = gofakes3.DefaultMaxBucketKeys
	}
	results := gofakes3.NewObjectList()
	seenPrefixes := make(map[string]struct{})
	marker := page.Marker
	var lastKey string
	count := 0
	for _, item := range objects {
		if marker != "" && item.key <= marker {
			continue
		}
		match := gofakes3.PrefixMatch{Key: item.key, MatchedPart: item.key}
		if prefix.HasPrefix || prefix.HasDelimiter {
			if !prefix.Match(item.key, &match) {
				continue
			}
		}
		if match.CommonPrefix {
			if _, ok := seenPrefixes[match.MatchedPart]; ok {
				continue
			}
			seenPrefixes[match.MatchedPart] = struct{}{}
			if count < limit {
				results.AddPrefix(match.MatchedPart)
				count++
			} else {
				results.IsTruncated = true
				lastKey = match.MatchedPart
				break
			}
			continue
		}
		if count < limit {
			results.Add(item.content)
			count++
			lastKey = item.key
		} else {
			results.IsTruncated = true
			lastKey = item.key
			break
		}
	}
	if results.IsTruncated {
		results.NextMarker = lastKey
	}
	return results, nil
}

func (b *Backend) CreateBucket(name string) error {
	if err := gofakes3.ValidateBucketName(name); err != nil {
		return err
	}
	if isReservedName(name) {
		return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
	}
	ctx := context.Background()
	if _, err := b.fs.Mkdir(ctx, b.bucketPath(name), fs.MkdirOptions{Parents: true, Mode: 0o755}); err != nil {
		if errors.Is(err, fs.ErrAlreadyExist) {
			return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
		}
		return err
	}
	return nil
}

func (b *Backend) BucketExists(name string) (bool, error) {
	ctx := context.Background()
	_, err := b.fs.Stat(ctx, b.bucketPath(name))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (b *Backend) DeleteBucket(name string) error {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, name); err != nil {
		return err
	}
	empty, err := b.bucketEmpty(ctx, name)
	if err != nil {
		return err
	}
	if !empty {
		return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
	}
	return b.fs.Remove(ctx, b.bucketPath(name), fs.RemoveOptions{})
}

func (b *Backend) ForceDeleteBucket(name string) error {
	ctx := context.Background()
	if _, err := b.fs.Stat(ctx, b.bucketPath(name)); err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return gofakes3.BucketNotFound(name)
		}
		return err
	}
	return b.fs.Remove(ctx, b.bucketPath(name), fs.RemoveOptions{Recursive: true, Force: true})
}

func (b *Backend) GetObject(bucket, object string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return nil, err
	}
	target := b.objectPath(bucket, object)
	fobj, err := b.fs.Stat(ctx, target)
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return nil, gofakes3.KeyNotFound(object)
		}
		return nil, err
	}
	rng, err := b.rangeForObject(rangeRequest, fobj.Size())
	if err != nil {
		return nil, err
	}
	return b.buildObjectResponse(ctx, object, fobj, rng, false)
}

func (b *Backend) HeadObject(bucket, object string) (*gofakes3.Object, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return nil, err
	}
	target := b.objectPath(bucket, object)
	fobj, err := b.fs.Stat(ctx, target)
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return nil, gofakes3.KeyNotFound(object)
		}
		return nil, err
	}
	return b.buildObjectResponse(ctx, object, fobj, nil, true)
}

func (b *Backend) DeleteObject(bucket, object string) (gofakes3.ObjectDeleteResult, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}
	target := b.objectPath(bucket, object)
	if err := b.fs.Remove(ctx, target, fs.RemoveOptions{}); err != nil && !errors.Is(err, fs.ErrNotFound) {
		return gofakes3.ObjectDeleteResult{}, err
	}
	return gofakes3.ObjectDeleteResult{}, nil
}

func (b *Backend) PutObject(bucket, key string, meta map[string]string, input io.Reader, _ int64, conditions *gofakes3.PutConditions) (gofakes3.PutObjectResult, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	target := b.objectPath(bucket, key)
	if err := b.ensureParent(ctx, target); err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	if conditions != nil {
		info, err := b.objectInfo(ctx, target)
		if err != nil {
			return gofakes3.PutObjectResult{}, err
		}
		if err := gofakes3.CheckPutConditions(conditions, info); err != nil {
			return gofakes3.PutObjectResult{}, err
		}
	}
	obj, err := b.fs.Create(ctx, target, fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	hasher := md5.New()
	reader := io.TeeReader(input, hasher)
	if _, err := obj.Write(ctx, reader, fs.IOOptions{}); err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	if err := obj.Flush(ctx); err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	if err := b.applyMetadata(ctx, target, meta, hasher.Sum(nil)); err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	return gofakes3.PutObjectResult{}, nil
}

func (b *Backend) DeleteMulti(bucket string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return gofakes3.MultiDeleteResult{}, err
	}
	var result gofakes3.MultiDeleteResult
	for _, key := range objects {
		if _, err := b.DeleteObject(bucket, key); err != nil {
			result.Error = append(result.Error, gofakes3.ErrorResultFromError(err))
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{Key: key})
		}
	}
	return result, result.AsError()
}

func (b *Backend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, srcBucket); err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	if err := b.ensureBucket(ctx, dstBucket); err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	src := b.objectPath(srcBucket, srcKey)
	dst := b.objectPath(dstBucket, dstKey)
	if err := b.ensureParent(ctx, dst); err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	obj, err := b.fs.Copy(ctx, src, dst, fs.CopyOptions{Overwrite: true})
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return gofakes3.CopyObjectResult{}, gofakes3.KeyNotFound(srcKey)
		}
		return gofakes3.CopyObjectResult{}, err
	}
	hash, err := b.ensureObjectHash(ctx, obj, obj.Metadata())
	if err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	if err := b.applyMetadata(ctx, dst, meta, hash); err != nil {
		return gofakes3.CopyObjectResult{}, err
	}
	return gofakes3.CopyObjectResult{
		ETag:         gofakes3.FormatETag(hash),
		LastModified: gofakes3.NewContentTime(obj.ModTime()),
	}, nil
}

func (b *Backend) CreateMultipartUpload(bucket, object string, meta map[string]string) (gofakes3.UploadID, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return "", err
	}
	if err := b.ensureUploadsDir(ctx, bucket); err != nil {
		return "", err
	}
	uploadID := b.nextUploadID()
	if _, err := b.fs.Mkdir(ctx, b.uploadDir(bucket, uploadID), fs.MkdirOptions{Parents: true, Mode: 0o755}); err != nil {
		if !errors.Is(err, fs.ErrAlreadyExist) {
			return "", err
		}
	}
	info := &uploadMetadata{
		Object:    object,
		Meta:      cloneMetadata(meta),
		Initiated: time.Now().UTC(),
		Parts:     make(map[int]uploadPartMetadata),
	}
	if err := b.saveUploadMetadata(ctx, bucket, uploadID, info); err != nil {
		return "", err
	}
	return uploadID, nil
}

func (b *Backend) UploadPart(bucket, object string, id gofakes3.UploadID, partNumber int, contentLength int64, input io.Reader) (string, error) {
	if partNumber <= 0 || partNumber > gofakes3.MaxUploadPartNumber {
		return "", gofakes3.ErrInvalidPart
	}
	ctx := context.Background()
	meta, err := b.loadUploadMetadata(ctx, bucket, id)
	if err != nil {
		return "", err
	}
	if meta.Object != object {
		return "", gofakes3.ErrNoSuchUpload
	}
	partPath := b.partPath(bucket, id, partNumber)
	if err := b.ensureParent(ctx, partPath); err != nil {
		return "", err
	}
	obj, err := b.fs.Create(ctx, partPath, fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		return "", err
	}
	hasher := md5.New()
	tee := io.TeeReader(input, hasher)
	written, err := obj.Write(ctx, tee, fs.IOOptions{})
	if err != nil {
		return "", err
	}
	if err := obj.Flush(ctx); err != nil {
		return "", err
	}
	if contentLength >= 0 && written != contentLength {
		return "", gofakes3.ErrIncompleteBody
	}
	etag := fmt.Sprintf(`"%s"`, hex.EncodeToString(hasher.Sum(nil)))
	meta.Parts[partNumber] = uploadPartMetadata{
		Path:         partPath,
		Size:         written,
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}
	if err := b.saveUploadMetadata(ctx, bucket, id, meta); err != nil {
		return "", err
	}
	return etag, nil
}

func (b *Backend) ListMultipartUploads(bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	ctx := context.Background()
	if err := b.ensureBucket(ctx, bucket); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = gofakes3.DefaultMaxBucketKeys
	}
	summaries, err := b.collectUploadSummaries(ctx, bucket)
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return &gofakes3.ListMultipartUploadsResult{Bucket: bucket, MaxUploads: limit}, nil
		}
		return nil, err
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Key == summaries[j].Key {
			return summaries[i].Initiated.Before(summaries[j].Initiated)
		}
		return summaries[i].Key < summaries[j].Key
	})
	start := 0
	if marker != nil {
		for idx, sum := range summaries {
			cmp := compareUpload(sum, marker.Object, marker.UploadID)
			if cmp <= 0 {
				start = idx + 1
			} else {
				break
			}
		}
	}
	result := &gofakes3.ListMultipartUploadsResult{
		Bucket:     bucket,
		Delimiter:  prefix.Delimiter,
		Prefix:     prefix.Prefix,
		MaxUploads: limit,
	}
	var match gofakes3.PrefixMatch
	seenPrefixes := make(map[string]bool)
	var count int64
	for idx := start; idx < len(summaries); idx++ {
		sum := summaries[idx]
		if prefix.HasPrefix || prefix.HasDelimiter {
			if !prefix.Match(sum.Key, &match) {
				continue
			}
			if match.CommonPrefix {
				if !seenPrefixes[match.MatchedPart] {
					result.CommonPrefixes = append(result.CommonPrefixes, match.AsCommonPrefix())
					seenPrefixes[match.MatchedPart] = true
				}
				continue
			}
		}
		result.Uploads = append(result.Uploads, gofakes3.ListMultipartUploadItem{
			Key:          sum.Key,
			UploadID:     sum.ID,
			StorageClass: "STANDARD",
			Initiated:    gofakes3.NewContentTime(sum.Initiated),
		})
		count++
		if count >= limit {
			if idx+1 < len(summaries) {
				result.IsTruncated = true
				result.NextKeyMarker = summaries[idx+1].Key
				result.NextUploadIDMarker = summaries[idx+1].ID
			}
			break
		}
	}
	return result, nil
}

func (b *Backend) ListParts(bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	ctx := context.Background()
	meta, err := b.loadUploadMetadata(ctx, bucket, uploadID)
	if err != nil {
		return nil, err
	}
	if meta.Object != object {
		return nil, gofakes3.ErrNoSuchUpload
	}
	if limit <= 0 {
		limit = gofakes3.DefaultMaxBucketKeys
	}
	result := &gofakes3.ListMultipartUploadPartsResult{
		Bucket:           bucket,
		Key:              object,
		UploadID:         uploadID,
		MaxParts:         limit,
		PartNumberMarker: marker,
		StorageClass:     "STANDARD",
	}
	if len(meta.Parts) == 0 {
		return result, nil
	}
	partNumbers := make([]int, 0, len(meta.Parts))
	for num := range meta.Parts {
		partNumbers = append(partNumbers, num)
	}
	sort.Ints(partNumbers)
	var count int64
	for _, num := range partNumbers {
		if num <= marker {
			continue
		}
		if count >= limit {
			result.IsTruncated = true
			result.NextPartNumberMarker = num
			break
		}
		part := meta.Parts[num]
		result.Parts = append(result.Parts, gofakes3.ListMultipartUploadPartItem{
			PartNumber:   num,
			ETag:         part.ETag,
			Size:         part.Size,
			LastModified: gofakes3.NewContentTime(part.LastModified),
		})
		count++
	}
	return result, nil
}

func (b *Backend) AbortMultipartUpload(bucket, object string, id gofakes3.UploadID) error {
	ctx := context.Background()
	meta, err := b.loadUploadMetadata(ctx, bucket, id)
	if err != nil {
		return err
	}
	if meta.Object != object {
		return gofakes3.ErrNoSuchUpload
	}
	return b.removeUpload(ctx, bucket, id)
}

func (b *Backend) CompleteMultipartUpload(bucket, object string, id gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (gofakes3.VersionID, string, error) {
	ctx := context.Background()
	if input == nil || len(input.Parts) == 0 {
		return "", "", gofakes3.ErrInvalidPart
	}
	meta, err := b.loadUploadMetadata(ctx, bucket, id)
	if err != nil {
		return "", "", err
	}
	if meta.Object != object {
		return "", "", gofakes3.ErrNoSuchUpload
	}
	for _, part := range input.Parts {
		info, ok := meta.Parts[part.PartNumber]
		if !ok {
			return "", "", gofakes3.ErrInvalidPart
		}
		if strings.Trim(part.ETag, "\"") != strings.Trim(info.ETag, "\"") {
			return "", "", gofakes3.ErrInvalidPart
		}
	}
	readers := make([]io.Reader, 0, len(input.Parts))
	var closers []io.Closer
	finalHash := md5.New()
	for _, part := range input.Parts {
		info := meta.Parts[part.PartNumber]
		obj, err := b.fs.Stat(ctx, info.Path)
		if err != nil {
			return "", "", err
		}
		reader := newObjectReader(ctx, obj, nil)
		readers = append(readers, reader)
		closers = append(closers, reader)
		hashBytes, err := hex.DecodeString(strings.Trim(info.ETag, "\""))
		if err != nil {
			return "", "", gofakes3.ErrInvalidPart
		}
		finalHash.Write(hashBytes)
	}
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()
	dest := b.objectPath(bucket, object)
	if err := b.ensureParent(ctx, dest); err != nil {
		return "", "", err
	}
	obj, err := b.fs.Create(ctx, dest, fs.CreateOptions{Mode: 0o644, Overwrite: true})
	if err != nil {
		return "", "", err
	}
	contentHash := md5.New()
	if _, err := obj.Write(ctx, io.TeeReader(io.MultiReader(readers...), contentHash), fs.IOOptions{}); err != nil {
		return "", "", err
	}
	if err := obj.Flush(ctx); err != nil {
		return "", "", err
	}
	if err := b.applyMetadata(ctx, dest, meta.Meta, contentHash.Sum(nil)); err != nil {
		return "", "", err
	}
	if err := b.removeUpload(ctx, bucket, id); err != nil {
		return "", "", err
	}
	etag := fmt.Sprintf(`"%s-%d"`, hex.EncodeToString(finalHash.Sum(nil)), len(input.Parts))
	return "", etag, nil
}

func (b *Backend) ensureBucket(ctx context.Context, name string) error {
	if name == "" || isReservedName(name) {
		return gofakes3.BucketNotFound(name)
	}
	if _, err := b.fs.Stat(ctx, b.bucketPath(name)); err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return gofakes3.BucketNotFound(name)
		}
		return err
	}
	return nil
}

func (b *Backend) bucketPath(name string) string {
	return path.Clean("/" + strings.TrimPrefix(name, "/"))
}

func (b *Backend) objectPath(bucket, key string) string {
	base := b.bucketPath(bucket)
	key = strings.TrimPrefix(key, "/")
	if key == "" {
		return base
	}
	return path.Clean(path.Join(base, key))
}

type listedObject struct {
	key     string
	content *gofakes3.Content
}

func (b *Backend) listObjects(ctx context.Context, bucket string) ([]listedObject, error) {
	ch, err := b.fs.List(ctx, b.bucketPath(bucket), fs.ListOptions{Recursive: true})
	if err != nil {
		return nil, err
	}
	base := b.bucketPath(bucket)
	var out []listedObject
	for entry := range ch {
		if entry.Object == nil {
			continue
		}
		if isInternalObject(entry.Object.Path()) {
			continue
		}
		key := strings.TrimPrefix(entry.Object.Path(), base+"/")
		if key == "" {
			continue
		}
		content, err := b.contentFromObject(ctx, base, entry.Object)
		if err != nil {
			return nil, err
		}
		out = append(out, listedObject{key: key, content: content})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].key < out[j].key
	})
	return out, nil
}

func (b *Backend) contentFromObject(ctx context.Context, bucketPath string, obj fs.Object) (*gofakes3.Content, error) {
	key := strings.TrimPrefix(obj.Path(), bucketPath+"/")
	meta := obj.Metadata()
	hash, err := b.ensureObjectHash(ctx, obj, meta)
	if err != nil {
		return nil, err
	}
	return &gofakes3.Content{
		Key:          key,
		LastModified: gofakes3.NewContentTime(obj.ModTime()),
		Size:         obj.Size(),
		ETag:         gofakes3.FormatETag(hash),
	}, nil
}

func (b *Backend) buildObjectResponse(ctx context.Context, key string, obj fs.Object, rng *gofakes3.ObjectRange, head bool) (*gofakes3.Object, error) {
	meta := obj.Metadata()
	hash, err := b.ensureObjectHash(ctx, obj, meta)
	if err != nil {
		return nil, err
	}
	headers := metadataFromObject(obj, meta)
	var body io.ReadCloser
	if head {
		body = io.NopCloser(bytes.NewReader(nil))
	} else {
		body = newObjectReader(ctx, obj, rng)
	}
	return &gofakes3.Object{
		Name:     key,
		Metadata: headers,
		Size:     obj.Size(),
		Contents: body,
		Hash:     hash,
		Range:    rng,
	}, nil
}

func metadataFromObject(obj fs.Object, meta fs.Metadata) map[string]string {
	headers := map[string]string{
		"Last-Modified": obj.ModTime().UTC().Format(http.TimeFormat),
	}
	if meta.Mode != 0 {
		headers["X-Amz-Meta-Posix-Mode"] = fmt.Sprintf("%o", meta.Mode)
	}
	headers["X-Amz-Meta-Posix-Uid"] = fmt.Sprintf("%d", meta.UID)
	headers["X-Amz-Meta-Posix-Gid"] = fmt.Sprintf("%d", meta.GID)
	for k, v := range meta.Extras {
		if k == etagXAttr {
			continue
		}
		headers[k] = v
	}
	return headers
}

func (b *Backend) hashFromMetadata(meta fs.Metadata) []byte {
	if meta.Extras == nil {
		return nil
	}
	if raw, ok := meta.Extras[etagXAttr]; ok {
		return []byte(raw)
	}
	return nil
}

func (b *Backend) ensureObjectHash(ctx context.Context, obj fs.Object, meta fs.Metadata) ([]byte, error) {
	if hash := b.hashFromMetadata(meta); len(hash) > 0 {
		return hash, nil
	}
	hash, err := b.computeObjectHash(ctx, obj)
	if err != nil {
		return nil, err
	}
	if len(hash) == 0 {
		return hash, nil
	}
	_ = b.fs.SetAttr(ctx, obj.Path(), vfs.AttrChanges{
		XAttrs: map[string][]byte{
			etagXAttr: hash,
		},
	}, vfs.User{})
	return hash, nil
}

func (b *Backend) computeObjectHash(ctx context.Context, obj fs.Object) ([]byte, error) {
	hasher := md5.New()
	buf := make([]byte, 256*1024)
	var offset int64
	for {
		n, err := obj.ReadAt(ctx, buf, offset, fs.IOOptions{})
		if n > 0 {
			if _, werr := hasher.Write(buf[:n]); werr != nil {
				return nil, werr
			}
			offset += int64(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	return hasher.Sum(nil), nil
}

func (b *Backend) applyMetadata(ctx context.Context, path string, meta map[string]string, hash []byte) error {
	cloned := cloneMetadata(meta)
	changes, err := parseAttrChanges(cloned)
	if err != nil {
		return err
	}
	if len(hash) > 0 {
		if changes.XAttrs == nil {
			changes.XAttrs = make(map[string][]byte)
		}
		changes.XAttrs[etagXAttr] = hash
	}
	if v, ok := cloned["Last-Modified"]; ok {
		delete(cloned, "Last-Modified")
		if changes.MTime == nil {
			if ts, err := time.Parse(http.TimeFormat, v); err == nil {
				changes.MTime = &ts
			}
		}
	}
	for k, v := range cloned {
		if changes.XAttrs == nil {
			changes.XAttrs = make(map[string][]byte)
		}
		changes.XAttrs[k] = []byte(v)
	}
	if emptyAttrChanges(changes) {
		return nil
	}
	return b.fs.SetAttr(ctx, path, changes, vfs.User{})
}

func cloneMetadata(meta map[string]string) map[string]string {
	if meta == nil {
		return nil
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func parseAttrChanges(meta map[string]string) (vfs.AttrChanges, error) {
	var changes vfs.AttrChanges
	if meta == nil {
		return changes, nil
	}
	if mode := meta["X-Amz-Meta-Posix-Mode"]; mode != "" {
		value, err := strconv.ParseUint(mode, 8, 32)
		if err != nil {
			return changes, fmt.Errorf("invalid posix mode: %w", err)
		}
		fileMode := os.FileMode(value)
		changes.Mode = &fileMode
		delete(meta, "X-Amz-Meta-Posix-Mode")
	}
	if uid := meta["X-Amz-Meta-Posix-Uid"]; uid != "" {
		value, err := strconv.ParseUint(uid, 10, 32)
		if err != nil {
			return changes, fmt.Errorf("invalid uid: %w", err)
		}
		u := uint32(value)
		changes.UID = &u
		delete(meta, "X-Amz-Meta-Posix-Uid")
	}
	if gid := meta["X-Amz-Meta-Posix-Gid"]; gid != "" {
		value, err := strconv.ParseUint(gid, 10, 32)
		if err != nil {
			return changes, fmt.Errorf("invalid gid: %w", err)
		}
		g := uint32(value)
		changes.GID = &g
		delete(meta, "X-Amz-Meta-Posix-Gid")
	}
	return changes, nil
}

func (b *Backend) bucketEmpty(ctx context.Context, bucket string) (bool, error) {
	ch, err := b.fs.List(ctx, b.bucketPath(bucket), fs.ListOptions{Recursive: true, Limit: 1, IncludeHidden: true})
	if err != nil {
		return false, err
	}
	for entry := range ch {
		switch {
		case entry.Dir != nil:
			dirPath := entry.Dir.Path()
			if dirPath == path.Join(b.bucketPath(bucket), uploadsDirName) || strings.Contains(dirPath, "/"+uploadsDirName+"/") {
				continue
			}
		case entry.Object != nil:
			if isInternalObject(entry.Object.Path()) {
				continue
			}
		}
		if entry.Name == uploadsDirName {
			continue
		}
		return false, nil
	}
	return true, nil
}

func (b *Backend) ensureParent(ctx context.Context, target string) error {
	parent := path.Dir(target)
	if parent == "." || parent == "/" {
		return nil
	}
	if _, err := b.fs.Stat(ctx, parent); err == nil {
		return nil
	}
	if _, err := b.fs.Mkdir(ctx, parent, fs.MkdirOptions{Parents: true, Mode: 0o755}); err != nil && !errors.Is(err, fs.ErrAlreadyExist) {
		return err
	}
	return nil
}

func (b *Backend) objectInfo(ctx context.Context, path string) (*gofakes3.ConditionalObjectInfo, error) {
	obj, err := b.fs.Stat(ctx, path)
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return &gofakes3.ConditionalObjectInfo{Exists: false}, nil
		}
		return nil, err
	}
	hash, err := b.ensureObjectHash(ctx, obj, obj.Metadata())
	if err != nil {
		return nil, err
	}
	return &gofakes3.ConditionalObjectInfo{
		Exists: true,
		Hash:   hash,
	}, nil
}

func (b *Backend) rangeForObject(req *gofakes3.ObjectRangeRequest, size int64) (*gofakes3.ObjectRange, error) {
	if req == nil {
		return nil, nil
	}
	return req.Range(size)
}

func isInternalObject(p string) bool {
	return strings.Contains(p, "/"+uploadsDirName+"/")
}

func isReservedName(name string) bool {
	return strings.HasPrefix(name, ".")
}

type objectReader struct {
	ctx       context.Context
	obj       fs.Object
	offset    int64
	remaining int64
}

func newObjectReader(ctx context.Context, obj fs.Object, rng *gofakes3.ObjectRange) io.ReadCloser {
	reader := &objectReader{
		ctx:       ctx,
		obj:       obj,
		offset:    0,
		remaining: obj.Size(),
	}
	if rng != nil {
		reader.offset = rng.Start
		reader.remaining = rng.Length
	}
	return reader
}

func (r *objectReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.obj.ReadAt(r.ctx, p, r.offset, fs.IOOptions{})
	r.offset += int64(n)
	r.remaining -= int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}
	if r.remaining == 0 {
		return n, io.EOF
	}
	return n, err
}

func (r *objectReader) Close() error {
	return nil
}

func emptyAttrChanges(changes vfs.AttrChanges) bool {
	return changes.Mode == nil &&
		changes.UID == nil &&
		changes.GID == nil &&
		changes.ATime == nil &&
		changes.MTime == nil &&
		changes.CTime == nil &&
		changes.Size == nil &&
		len(changes.XAttrs) == 0
}

type uploadMetadata struct {
	Object    string                     `json:"object"`
	Meta      map[string]string          `json:"meta"`
	Initiated time.Time                  `json:"initiated"`
	Parts     map[int]uploadPartMetadata `json:"parts"`
}

type uploadPartMetadata struct {
	Path         string    `json:"path"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
}

type uploadSummary struct {
	Key       string
	ID        gofakes3.UploadID
	Initiated time.Time
}

func compareUpload(sum uploadSummary, key string, id gofakes3.UploadID) int {
	if sum.Key < key {
		return -1
	}
	if sum.Key > key {
		return 1
	}
	return strings.Compare(string(sum.ID), string(id))
}

func (b *Backend) ensureUploadsDir(ctx context.Context, bucket string) error {
	if _, err := b.fs.Mkdir(ctx, b.uploadsDir(bucket), fs.MkdirOptions{Parents: true, Mode: 0o755}); err != nil && !errors.Is(err, fs.ErrAlreadyExist) {
		return err
	}
	return nil
}

func (b *Backend) uploadsDir(bucket string) string {
	return path.Join(b.bucketPath(bucket), uploadsDirName)
}

func (b *Backend) uploadDir(bucket string, id gofakes3.UploadID) string {
	return path.Join(b.uploadsDir(bucket), string(id))
}

func (b *Backend) uploadMetaPath(bucket string, id gofakes3.UploadID) string {
	return path.Join(b.uploadDir(bucket, id), uploadMetaFile)
}

func (b *Backend) partPath(bucket string, id gofakes3.UploadID, partNumber int) string {
	return path.Join(b.uploadDir(bucket, id), fmt.Sprintf(partFilePattern, partNumber))
}

func (b *Backend) loadUploadMetadata(ctx context.Context, bucket string, id gofakes3.UploadID) (*uploadMetadata, error) {
	obj, err := b.fs.Stat(ctx, b.uploadMetaPath(bucket, id))
	if err != nil {
		if errors.Is(err, fs.ErrNotFound) {
			return nil, gofakes3.ErrNoSuchUpload
		}
		return nil, err
	}
	reader := newObjectReader(ctx, obj, nil)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	info := &uploadMetadata{}
	if err := json.Unmarshal(data, info); err != nil {
		return nil, err
	}
	if info.Parts == nil {
		info.Parts = make(map[int]uploadPartMetadata)
	}
	if info.Meta == nil {
		info.Meta = map[string]string{}
	}
	return info, nil
}

func (b *Backend) saveUploadMetadata(ctx context.Context, bucket string, id gofakes3.UploadID, meta *uploadMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	obj, err := b.fs.Create(ctx, b.uploadMetaPath(bucket, id), fs.CreateOptions{Mode: 0o600, Overwrite: true})
	if err != nil {
		return err
	}
	if _, err := obj.Write(ctx, bytes.NewReader(data), fs.IOOptions{}); err != nil {
		return err
	}
	return obj.Flush(ctx)
}

func (b *Backend) removeUpload(ctx context.Context, bucket string, id gofakes3.UploadID) error {
	return b.fs.Remove(ctx, b.uploadDir(bucket, id), fs.RemoveOptions{Recursive: true, Force: true})
}

func (b *Backend) nextUploadID() gofakes3.UploadID {
	seq := atomic.AddUint64(&b.uploadSeq, 1)
	return gofakes3.UploadID(fmt.Sprintf("%020d", seq))
}

func (b *Backend) collectUploadSummaries(ctx context.Context, bucket string) ([]uploadSummary, error) {
	ch, err := b.fs.List(ctx, b.uploadsDir(bucket), fs.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []uploadSummary
	for entry := range ch {
		if entry.Dir == nil {
			continue
		}
		id := gofakes3.UploadID(path.Base(entry.Dir.Path()))
		meta, err := b.loadUploadMetadata(ctx, bucket, id)
		if err != nil {
			return nil, err
		}
		out = append(out, uploadSummary{
			Key:       meta.Object,
			ID:        id,
			Initiated: meta.Initiated,
		})
	}
	return out, nil
}
