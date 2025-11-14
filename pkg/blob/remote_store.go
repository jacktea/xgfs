package blob

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jacktea/xgfs/pkg/cache"
	"github.com/jacktea/xgfs/pkg/encryption"
)

// RemoteStore persists shards in object storage compatible with S3/OSS/COS.
type RemoteStore struct {
	client  *http.Client
	baseURL string
	signer  Signer
	cache   *cache.Cache
}

// RemoteConfig is a generic configuration used by provider helpers.
type RemoteConfig struct {
	Endpoint     string
	Bucket       string
	Client       *http.Client
	CacheEntries int
	CacheTTL     time.Duration
}

// Signer signs HTTP requests for remote providers.
type Signer interface {
	Sign(req *http.Request, payloadHash string) error
}

// NewRemoteStore builds a RemoteStore with a signer.
func NewRemoteStore(cfg RemoteConfig, signer Signer) (*RemoteStore, error) {
	if cfg.Endpoint == "" || cfg.Bucket == "" {
		return nil, fmt.Errorf("remote store requires endpoint and bucket")
	}
	bucket := strings.Trim(cfg.Bucket, "/")
	if bucket == "" {
		return nil, fmt.Errorf("remote store bucket invalid")
	}
	base := strings.TrimSuffix(cfg.Endpoint, "/") + "/" + bucket
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	var shardCache *cache.Cache
	if cfg.CacheEntries == 0 {
		cfg.CacheEntries = 512
	}
	if cfg.CacheEntries > 0 {
		shardCache = cache.New(cfg.CacheEntries, cfg.CacheTTL)
	}
	return &RemoteStore{client: client, baseURL: base, signer: signer, cache: shardCache}, nil
}

// Put uploads a shard via HTTP PUT.
func (r *RemoteStore) Put(ctx context.Context, src io.Reader, size int64, opts PutOptions) (ID, int64, error) {
	var plain bytes.Buffer
	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(&plain, hasher), src); err != nil {
		return "", 0, err
	}
	sum := hex.EncodeToString(hasher.Sum(nil))
	exists, err := r.objectExists(ctx, ID(sum))
	if err != nil {
		return "", 0, err
	}
	if exists {
		return ID(sum), int64(plain.Len()), nil
	}
	payload, err := buildPayload(plain.Bytes(), opts)
	if err != nil {
		return "", 0, err
	}
	md5Sum := md5.Sum(payload)
	payloadDigest := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(payloadDigest[:])
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.objectURL(sum), bytes.NewReader(payload))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.Itoa(len(payload)))
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5Sum[:]))
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("Host", req.URL.Host)
	if err := r.signer.Sign(req, payloadHash); err != nil {
		return "", 0, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", 0, fmt.Errorf("remote put %s: %s", resp.Status, string(body))
	}
	return ID(sum), int64(plain.Len()), nil
}

// Get retrieves a shard via HTTP GET.
func (r *RemoteStore) Get(ctx context.Context, id ID) (io.ReadCloser, int64, error) {
	if data, ok := r.cacheGet(id); ok {
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.objectURL(string(id)), nil)
	if err != nil {
		return nil, 0, err
	}
	payloadHash := emptyPayloadHash()
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("Host", req.URL.Host)
	if err := r.signer.Sign(req, payloadHash); err != nil {
		return nil, 0, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, 0, fmt.Errorf("remote get %s: %s", resp.Status, string(body))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	r.cachePut(id, data)
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

// Delete removes a shard.
func (r *RemoteStore) Delete(ctx context.Context, id ID) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.objectURL(string(id)), nil)
	if err != nil {
		return err
	}
	payloadHash := emptyPayloadHash()
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("Host", req.URL.Host)
	if err := r.signer.Sign(req, payloadHash); err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("remote delete %s: %s", resp.Status, string(body))
	}
	return nil
}

// Exists checks whether the shard is already stored.
func (r *RemoteStore) Exists(ctx context.Context, id ID) (bool, error) {
	return r.objectExists(ctx, id)
}

func (r *RemoteStore) objectExists(ctx context.Context, id ID) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, r.objectURL(string(id)), nil)
	if err != nil {
		return false, err
	}
	payloadHash := emptyPayloadHash()
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("Host", req.URL.Host)
	if err := r.signer.Sign(req, payloadHash); err != nil {
		return false, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("remote head %s", resp.Status)
}

func (r *RemoteStore) objectURL(object string) string {
	return strings.TrimSuffix(r.baseURL, "/") + "/" + object
}

func (r *RemoteStore) cacheGet(id ID) ([]byte, bool) {
	if r == nil || r.cache == nil {
		return nil, false
	}
	if v, ok := r.cache.Get(string(id)); ok {
		if data, ok := v.([]byte); ok {
			return append([]byte(nil), data...), true
		}
	}
	return nil, false
}

func (r *RemoteStore) cachePut(id ID, data []byte) {
	if r == nil || r.cache == nil || len(data) == 0 {
		return
	}
	r.cache.Set(string(id), append([]byte(nil), data...))
}

func buildPayload(data []byte, opts PutOptions) ([]byte, error) {
	if !opts.Encryption.Enabled() {
		return data, nil
	}
	return encryption.Encrypt(data, opts.Encryption)
}

func emptyPayloadHash() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}

// S3Config describes the parameters for AWS S3-compatible stores.
type S3Config struct {
	RemoteConfig
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

// NewS3Store builds a RemoteStore with AWS SigV4 signing.
func NewS3Store(cfg S3Config) (*RemoteStore, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" {
		return nil, fmt.Errorf("s3 store requires access key, secret key, and region")
	}
	signer := &s3Signer{
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
		region:    cfg.Region,
		token:     cfg.SessionToken,
	}
	return NewRemoteStore(cfg.RemoteConfig, signer)
}

// OSSConfig describes the parameters for Aliyun OSS.
type OSSConfig struct {
	RemoteConfig
	AccessKey string
	SecretKey string
}

// NewOSSStore builds a RemoteStore for Aliyun OSS.
func NewOSSStore(cfg OSSConfig) (*RemoteStore, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("oss store requires access key and secret key")
	}
	signer := &ossSigner{
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
	}
	return NewRemoteStore(cfg.RemoteConfig, signer)
}

// COSConfig describes Tencent COS parameters.
type COSConfig struct {
	RemoteConfig
	AccessKey string
	SecretKey string
}

// NewCOSStore builds a RemoteStore for Tencent COS.
func NewCOSStore(cfg COSConfig) (*RemoteStore, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("cos store requires access key and secret key")
	}
	signer := &cosSigner{
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
	}
	return NewRemoteStore(cfg.RemoteConfig, signer)
}

// --- Signer implementations ---

type s3Signer struct {
	accessKey string
	secretKey string
	region    string
	token     string
	now       func() time.Time
}

func (s *s3Signer) Sign(req *http.Request, payloadHash string) error {
	if s.now == nil {
		s.now = time.Now
	}
	t := s.now().UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("host", req.URL.Host)
	if payloadHash == "" {
		payloadHash = emptyPayloadHash()
	}
	canonicalURI := canonicalURI(req.URL)
	canonicalQuery := canonicalQueryString(req.URL)
	canonicalHeaders, signedHeaders := canonicalHeaderStrings(req.Header)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	hashedRequest := sha256.Sum256([]byte(canonicalRequest))
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, s.region)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(hashedRequest[:]),
	}, "\n")
	signingKey := s.deriveKey(dateStamp)
	signature := hmacSHA256Hex(signingKey, stringToSign)
	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKey, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
	if s.token != "" {
		req.Header.Set("x-amz-security-token", s.token)
	}
	return nil
}

func (s *s3Signer) deriveKey(date string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+s.secretKey), date)
	kRegion := hmacSHA256(kDate, s.region)
	kService := hmacSHA256(kRegion, "s3")
	return hmacSHA256(kService, "aws4_request")
}

type ossSigner struct {
	accessKey string
	secretKey string
}

func (o *ossSigner) Sign(req *http.Request, payloadHash string) error {
	date := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("Date", date)
	contentMD5 := req.Header.Get("Content-MD5")
	contentType := req.Header.Get("Content-Type")
	canonicalHeaders := ossCanonicalHeaders(req.Header)
	resource := req.URL.EscapedPath()
	stringToSign := strings.Join([]string{
		req.Method,
		contentMD5,
		contentType,
		date,
		canonicalHeaders + resource,
	}, "\n")
	mac := hmac.New(sha1.New, []byte(o.secretKey))
	mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	req.Header.Set("Authorization", fmt.Sprintf("OSS %s:%s", o.accessKey, signature))
	return nil
}

type cosSigner struct {
	accessKey string
	secretKey string
	now       func() time.Time
}

func (c *cosSigner) Sign(req *http.Request, payloadHash string) error {
	if c.now == nil {
		c.now = time.Now
	}
	now := c.now()
	start := now.Add(-1 * time.Minute).Unix()
	end := now.Add(15 * time.Minute).Unix()
	signTime := fmt.Sprintf("%d;%d", start, end)
	headerList, canonicalHeaders := cosCanonicalHeaders(req.Header)
	queryList, canonicalQuery := cosCanonicalQuery(req.URL)
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	httpString := strings.Join([]string{
		strings.ToLower(req.Method),
		path,
		canonicalQuery,
		canonicalHeaders,
	}, "\n")
	httpHash := sha1.Sum([]byte(httpString))
	stringToSign := fmt.Sprintf("sha1\n%s\n%x\n", signTime, httpHash)
	signKey := hmacSHA1([]byte(c.secretKey), signTime)
	signature := hmacSHA1(signKey, stringToSign)
	auth := fmt.Sprintf("q-sign-algorithm=sha1&q-ak=%s&q-sign-time=%s&q-key-time=%s&q-header-list=%s&q-url-param-list=%s&q-signature=%s",
		c.accessKey, signTime, signTime, headerList, queryList, hex.EncodeToString(signature))
	req.Header.Set("Authorization", auth)
	return nil
}

func canonicalURI(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func canonicalQueryString(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}
	values, _ := url.ParseQuery(u.RawQuery)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := values[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(v)))
		}
	}
	return strings.Join(parts, "&")
}

func canonicalHeaderStrings(h http.Header) (string, string) {
	keys := make([]string, 0, len(h))
	lower := make(map[string][]string)
	for k, v := range h {
		lk := strings.ToLower(k)
		keys = append(keys, lk)
		lower[lk] = v
	}
	sort.Strings(keys)
	keys = unique(keys)
	var canonical []string
	var signed []string
	for _, k := range keys {
		values := make([]string, len(lower[k]))
		copy(values, lower[k])
		sort.Strings(values)
		trimmed := strings.Join(values, ",")
		canonical = append(canonical, fmt.Sprintf("%s:%s", k, strings.TrimSpace(trimmed)))
		signed = append(signed, k)
	}
	return strings.Join(canonical, "\n") + "\n", strings.Join(signed, ";")
}

func ossCanonicalHeaders(h http.Header) string {
	type kv struct {
		key   string
		value string
	}
	var headers []kv
	for k, v := range h {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-oss-") {
			headers = append(headers, kv{key: lk, value: strings.Join(v, ",")})
		}
	}
	sort.Slice(headers, func(i, j int) bool {
		return headers[i].key < headers[j].key
	})
	var b strings.Builder
	for _, header := range headers {
		fmt.Fprintf(&b, "%s:%s\n", header.key, header.value)
	}
	return b.String()
}

func cosCanonicalHeaders(h http.Header) (string, string) {
	var keys []string
	values := make(map[string][]string)
	for k, v := range h {
		lk := strings.ToLower(k)
		keys = append(keys, lk)
		values[lk] = append([]string(nil), v...)
	}
	sort.Strings(keys)
	keys = unique(keys)
	var parts []string
	for _, k := range keys {
		vs := values[k]
		sort.Strings(vs)
		joined := url.QueryEscape(strings.Join(vs, ","))
		parts = append(parts, fmt.Sprintf("%s=%s", k, joined))
	}
	return strings.Join(keys, ";"), strings.Join(parts, "&")
}

func cosCanonicalQuery(u *url.URL) (string, string) {
	if u.RawQuery == "" {
		return "", ""
	}
	raw := u.Query()
	keys := make([]string, 0, len(raw))
	values := make(map[string][]string)
	for k, v := range raw {
		lk := strings.ToLower(k)
		keys = append(keys, lk)
		cp := append([]string(nil), v...)
		values[lk] = cp
	}
	sort.Strings(keys)
	keys = unique(keys)
	var parts []string
	for _, k := range keys {
		vs := values[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, fmt.Sprintf("%s=%s", k, url.QueryEscape(v)))
		}
	}
	return strings.Join(keys, ";"), strings.Join(parts, "&")
}

func unique(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := []string{in[0]}
	for i := 1; i < len(in); i++ {
		if in[i] != in[i-1] {
			out = append(out, in[i])
		}
	}
	return out
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func hmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

func hmacSHA1Hex(key []byte, data string) string {
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func hmacSHA1(key []byte, data string) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
