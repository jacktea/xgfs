package cache

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

// Cache is a threadsafe LRU with TTL support.
type Cache struct {
	mu       sync.Mutex
	ll       *list.List
	items    map[string]*list.Element
	capacity int
	ttl      time.Duration
}

type entry struct {
	key    string
	value  any
	expire time.Time
}

// New returns a cache with given capacity and ttl.
func New(capacity int, ttl time.Duration) *Cache {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Cache{
		ll:       list.New(),
		items:    make(map[string]*list.Element),
		capacity: capacity,
		ttl:      ttl,
	}
}

// Get retrieves a value if present and not expired.
func (c *Cache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.items[key]; ok {
		ent := ele.Value.(*entry)
		if c.ttl > 0 && time.Now().After(ent.expire) {
			c.removeElement(ele)
			return nil, false
		}
		c.ll.MoveToFront(ele)
		return ent.value, true
	}
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
	ent := &entry{key: key, value: value}
	if c.ttl > 0 {
		ent.expire = time.Now().Add(c.ttl)
	}
	ele := c.ll.PushFront(ent)
	c.items[key] = ele
	if c.ll.Len() > c.capacity {
		c.evictOldest()
	}
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
	if prefix == "" {
		c.Clear()
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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
}

func (c *Cache) evictOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *Cache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	ent := ele.Value.(*entry)
	delete(c.items, ent.key)
}
