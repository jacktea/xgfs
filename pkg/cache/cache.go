package cache

import (
	"container/list"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Stats holds cache statistics.
type Stats struct {
	Hits      int64 // Number of cache hits
	Misses    int64 // Number of cache misses
	Size      int   // Current number of entries
	Capacity  int   // Maximum capacity
	Evictions int64 // Number of evicted entries
	Expired   int64 // Number of expired entries
}

// Cache is a threadsafe LRU with TTL support.
type Cache struct {
	mu          sync.RWMutex
	ll          *list.List
	items       map[string]*list.Element
	capacity    int
	ttl         time.Duration
	stats       Stats
	cleanupStop context.CancelFunc
	cleanupDone chan struct{}
}

type entry struct {
	key    string
	value  any
	expire time.Time
}

// New returns a cache with given capacity and ttl.
// If ttl > 0, starts a background goroutine to periodically clean expired entries.
func New(capacity int, ttl time.Duration) *Cache {
	if capacity <= 0 {
		capacity = 1024
	}
	c := &Cache{
		ll:       list.New(),
		items:    make(map[string]*list.Element),
		capacity: capacity,
		ttl:      ttl,
	}
	if ttl > 0 {
		c.cleanupDone = make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		c.cleanupStop = cancel
		go c.cleanupExpired(ctx, ttl)
	}
	return c
}

// Get retrieves a value if present and not expired.
func (c *Cache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		ent := ele.Value.(*entry)
		if c.ttl > 0 && time.Now().After(ent.expire) {
			c.removeElement(ele)
			atomic.AddInt64(&c.stats.Expired, 1)
			atomic.AddInt64(&c.stats.Misses, 1)
			return nil, false
		}
		c.ll.MoveToFront(ele)
		atomic.AddInt64(&c.stats.Hits, 1)
		return ent.value, true
	}
	atomic.AddInt64(&c.stats.Misses, 1)
	return nil, false
}

// Set inserts or updates a cache entry.
func (c *Cache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		ent := ele.Value.(*entry)
		ent.value = value
		if c.ttl > 0 {
			ent.expire = time.Now().Add(c.ttl)
		}
		return
	}
	// Evict oldest entry if at capacity before adding new one
	if c.ll.Len() >= c.capacity {
		c.evictOldest()
	}
	ent := &entry{key: key, value: value}
	if c.ttl > 0 {
		ent.expire = time.Now().Add(c.ttl)
	}
	ele := c.ll.PushFront(ent)
	c.items[key] = ele
}

// Delete removes a key if present.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		c.removeElement(ele)
	}
}

// DeletePrefix removes all keys with the given prefix.
func (c *Cache) DeletePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prefix == "" {
		// Clear all entries without calling Clear() to avoid deadlock
		for _, ele := range c.items {
			c.ll.Remove(ele)
		}
		c.items = make(map[string]*list.Element)
		c.ll = list.New()
		return
	}
	for key, ele := range c.items {
		if strings.HasPrefix(key, prefix) {
			c.removeElement(ele)
		}
	}
}

// Clear removes all entries.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ele := range c.items {
		c.ll.Remove(ele)
	}
	c.items = make(map[string]*list.Element)
	c.ll = list.New()
}

func (c *Cache) evictOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
		atomic.AddInt64(&c.stats.Evictions, 1)
	}
}

func (c *Cache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	ent := ele.Value.(*entry)
	delete(c.items, ent.key)
}

// Stats returns current cache statistics.
func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := Stats{
		Hits:      atomic.LoadInt64(&c.stats.Hits),
		Misses:    atomic.LoadInt64(&c.stats.Misses),
		Size:      c.ll.Len(),
		Capacity:  c.capacity,
		Evictions: atomic.LoadInt64(&c.stats.Evictions),
		Expired:   atomic.LoadInt64(&c.stats.Expired),
	}
	return s
}

// Size returns the current number of entries in the cache.
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ll.Len()
}

// cleanupExpired periodically removes expired entries.
func (c *Cache) cleanupExpired(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval / 2)
	if interval < time.Minute {
		ticker = time.NewTicker(time.Minute)
	}
	defer ticker.Stop()
	defer close(c.cleanupDone)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cleanupOnce()
		}
	}
}

// cleanupOnce removes all expired entries in one pass.
func (c *Cache) cleanupOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ttl <= 0 {
		return
	}
	now := time.Now()
	var expired []*list.Element
	for _, ele := range c.items {
		ent := ele.Value.(*entry)
		if now.After(ent.expire) {
			expired = append(expired, ele)
		}
	}
	for _, ele := range expired {
		c.removeElement(ele)
		atomic.AddInt64(&c.stats.Expired, 1)
	}
}

// Close stops the background cleanup goroutine and waits for it to finish.
// It's safe to call Close multiple times.
func (c *Cache) Close() error {
	if c.cleanupStop != nil {
		c.cleanupStop()
		c.cleanupStop = nil
		if c.cleanupDone != nil {
			<-c.cleanupDone
		}
	}
	return nil
}
