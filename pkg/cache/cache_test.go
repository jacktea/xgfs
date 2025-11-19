package cache

import (
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Run("valid capacity", func(t *testing.T) {
		c := New(100, 0)
		if c == nil {
			t.Fatal("expected non-nil cache")
		}
		if c.capacity != 100 {
			t.Errorf("expected capacity 100, got %d", c.capacity)
		}
		if c.ttl != 0 {
			t.Errorf("expected ttl 0, got %v", c.ttl)
		}
	})

	t.Run("zero capacity uses default", func(t *testing.T) {
		c := New(0, 0)
		if c.capacity != 1024 {
			t.Errorf("expected default capacity 1024, got %d", c.capacity)
		}
	})

	t.Run("negative capacity uses default", func(t *testing.T) {
		c := New(-1, 0)
		if c.capacity != 1024 {
			t.Errorf("expected default capacity 1024, got %d", c.capacity)
		}
	})

	t.Run("with ttl starts cleanup goroutine", func(t *testing.T) {
		c := New(100, 100*time.Millisecond)
		if c.cleanupStop == nil {
			t.Error("expected cleanup goroutine to be started")
		}
		c.Close()
	})

	t.Run("without ttl no cleanup goroutine", func(t *testing.T) {
		c := New(100, 0)
		if c.cleanupStop != nil {
			t.Error("expected no cleanup goroutine when ttl is 0")
		}
	})
}

func TestGetSet(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	t.Run("set and get", func(t *testing.T) {
		c.Set("key1", "value1")
		val, ok := c.Get("key1")
		if !ok {
			t.Fatal("expected key1 to be found")
		}
		if val != "value1" {
			t.Errorf("expected value1, got %v", val)
		}
	})

	t.Run("get non-existent key", func(t *testing.T) {
		_, ok := c.Get("nonexistent")
		if ok {
			t.Error("expected key to not be found")
		}
	})

	t.Run("update existing key", func(t *testing.T) {
		c.Set("key2", "value2")
		c.Set("key2", "value2-updated")
		val, ok := c.Get("key2")
		if !ok {
			t.Fatal("expected key2 to be found")
		}
		if val != "value2-updated" {
			t.Errorf("expected value2-updated, got %v", val)
		}
	})

	t.Run("different value types", func(t *testing.T) {
		c.Set("int", 42)
		c.Set("string", "hello")
		c.Set("bool", true)
		c.Set("slice", []int{1, 2, 3})

		if val, _ := c.Get("int"); val != 42 {
			t.Errorf("expected 42, got %v", val)
		}
		if val, _ := c.Get("string"); val != "hello" {
			t.Errorf("expected hello, got %v", val)
		}
		if val, _ := c.Get("bool"); val != true {
			t.Errorf("expected true, got %v", val)
		}
		if val, _ := c.Get("slice"); val == nil {
			t.Error("expected slice to be found")
		}
	})
}

func TestLRUEviction(t *testing.T) {
	c := New(3, 0)
	defer c.Close()

	// Fill cache to capacity
	c.Set("key1", "value1")
	c.Set("key2", "value2")
	c.Set("key3", "value3")

	// Verify all keys are present
	if _, ok := c.Get("key1"); !ok {
		t.Error("key1 should be present")
	}
	if _, ok := c.Get("key2"); !ok {
		t.Error("key2 should be present")
	}
	if _, ok := c.Get("key3"); !ok {
		t.Error("key3 should be present")
	}

	// Access key1 to make it most recently used
	c.Get("key1")

	// Add new key, should evict key2 (least recently used)
	c.Set("key4", "value4")

	// key2 should be evicted
	if _, ok := c.Get("key2"); ok {
		t.Error("key2 should have been evicted")
	}

	// key1, key3, key4 should still be present
	if _, ok := c.Get("key1"); !ok {
		t.Error("key1 should still be present")
	}
	if _, ok := c.Get("key3"); !ok {
		t.Error("key3 should still be present")
	}
	if _, ok := c.Get("key4"); !ok {
		t.Error("key4 should be present")
	}

	stats := c.Stats()
	if stats.Evictions != 1 {
		t.Errorf("expected 1 eviction, got %d", stats.Evictions)
	}
}

func TestTTLExpiration(t *testing.T) {
	t.Run("expired entry on get", func(t *testing.T) {
		c := New(10, 50*time.Millisecond)
		defer c.Close()

		c.Set("key1", "value1")
		time.Sleep(100 * time.Millisecond)

		_, ok := c.Get("key1")
		if ok {
			t.Error("expected key1 to be expired")
		}

		stats := c.Stats()
		if stats.Expired == 0 {
			t.Error("expected expired count to be > 0")
		}
		if stats.Misses == 0 {
			t.Error("expected misses count to be > 0")
		}
	})

	t.Run("non-expired entry", func(t *testing.T) {
		c := New(10, 200*time.Millisecond)
		defer c.Close()

		c.Set("key1", "value1")
		time.Sleep(50 * time.Millisecond)

		val, ok := c.Get("key1")
		if !ok {
			t.Error("expected key1 to still be valid")
		}
		if val != "value1" {
			t.Errorf("expected value1, got %v", val)
		}
	})

	t.Run("background cleanup", func(t *testing.T) {
		// Use TTL >= 1 minute so cleanup interval is TTL/2 instead of 1 minute
		ttl := 2 * time.Minute
		c := New(10, ttl)
		defer c.Close()

		c.Set("key1", "value1")
		c.Set("key2", "value2")

		// Wait for entries to expire
		time.Sleep(ttl + 100*time.Millisecond)

		// Wait for background cleanup (interval is ttl/2 = 1 minute)
		time.Sleep(90 * time.Second)

		stats := c.Stats()
		if stats.Size > 0 {
			t.Errorf("expected cache to be empty after cleanup, got size %d", stats.Size)
		}
		if stats.Expired == 0 {
			t.Error("expected expired count to be > 0")
		}
	})

	t.Run("no ttl means no expiration", func(t *testing.T) {
		c := New(10, 0)
		defer c.Close()

		c.Set("key1", "value1")
		time.Sleep(100 * time.Millisecond)

		val, ok := c.Get("key1")
		if !ok {
			t.Error("expected key1 to still be valid without ttl")
		}
		if val != "value1" {
			t.Errorf("expected value1, got %v", val)
		}
	})
}

func TestDelete(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	c.Set("key1", "value1")
	c.Set("key2", "value2")

	t.Run("delete existing key", func(t *testing.T) {
		c.Delete("key1")
		if _, ok := c.Get("key1"); ok {
			t.Error("expected key1 to be deleted")
		}
		if _, ok := c.Get("key2"); !ok {
			t.Error("expected key2 to still exist")
		}
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		// Should not panic
		c.Delete("nonexistent")
	})

	t.Run("delete and size", func(t *testing.T) {
		c.Set("key3", "value3")
		if c.Size() != 2 {
			t.Errorf("expected size 2, got %d", c.Size())
		}
		c.Delete("key2")
		if c.Size() != 1 {
			t.Errorf("expected size 1, got %d", c.Size())
		}
	})
}

func TestDeletePrefix(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	c.Set("user:1", "value1")
	c.Set("user:2", "value2")
	c.Set("admin:1", "value3")
	c.Set("item:1", "value4")

	t.Run("delete by prefix", func(t *testing.T) {
		c.DeletePrefix("user:")
		if _, ok := c.Get("user:1"); ok {
			t.Error("expected user:1 to be deleted")
		}
		if _, ok := c.Get("user:2"); ok {
			t.Error("expected user:2 to be deleted")
		}
		if _, ok := c.Get("admin:1"); !ok {
			t.Error("expected admin:1 to still exist")
		}
		if _, ok := c.Get("item:1"); !ok {
			t.Error("expected item:1 to still exist")
		}
	})

	t.Run("empty prefix clears all", func(t *testing.T) {
		c.Set("key1", "value1")
		c.Set("key2", "value2")
		c.DeletePrefix("")
		if c.Size() != 0 {
			t.Errorf("expected size 0, got %d", c.Size())
		}
	})

	t.Run("delete non-matching prefix", func(t *testing.T) {
		c.Set("key1", "value1")
		c.Set("key2", "value2")
		c.DeletePrefix("xyz:")
		if c.Size() != 2 {
			t.Errorf("expected size 2, got %d", c.Size())
		}
	})
}

func TestClear(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	c.Set("key1", "value1")
	c.Set("key2", "value2")
	c.Set("key3", "value3")

	c.Clear()

	if c.Size() != 0 {
		t.Errorf("expected size 0, got %d", c.Size())
	}

	if _, ok := c.Get("key1"); ok {
		t.Error("expected key1 to be cleared")
	}

	c.Set("key4", "value4")
	if c.Size() != 1 {
		t.Errorf("expected size 1 after clear and add, got %d", c.Size())
	}
}

func TestStats(t *testing.T) {
	c := New(3, 0)
	defer c.Close()

	initial := c.Stats()
	if initial.Hits != 0 || initial.Misses != 0 || initial.Evictions != 0 {
		t.Error("initial stats should be zero")
	}
	if initial.Capacity != 3 {
		t.Errorf("expected capacity 3, got %d", initial.Capacity)
	}

	// Generate some hits
	c.Set("key1", "value1")
	c.Get("key1")
	c.Get("key1")
	c.Get("key1")

	// Generate some misses
	c.Get("nonexistent1")
	c.Get("nonexistent2")

	// Generate evictions
	c.Set("key2", "value2")
	c.Set("key3", "value3")
	c.Set("key4", "value4") // Should evict key1

	stats := c.Stats()
	if stats.Hits != 3 {
		t.Errorf("expected 3 hits, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Errorf("expected 2 misses, got %d", stats.Misses)
	}
	if stats.Evictions != 1 {
		t.Errorf("expected 1 eviction, got %d", stats.Evictions)
	}
	if stats.Size != 3 {
		t.Errorf("expected size 3, got %d", stats.Size)
	}
	if stats.Capacity != 3 {
		t.Errorf("expected capacity 3, got %d", stats.Capacity)
	}
}

func TestSize(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	if c.Size() != 0 {
		t.Errorf("expected initial size 0, got %d", c.Size())
	}

	c.Set("key1", "value1")
	if c.Size() != 1 {
		t.Errorf("expected size 1, got %d", c.Size())
	}

	c.Set("key2", "value2")
	if c.Size() != 2 {
		t.Errorf("expected size 2, got %d", c.Size())
	}

	c.Set("key1", "value1-updated") // Update existing
	if c.Size() != 2 {
		t.Errorf("expected size 2 after update, got %d", c.Size())
	}

	c.Delete("key1")
	if c.Size() != 1 {
		t.Errorf("expected size 1 after delete, got %d", c.Size())
	}
}

func TestConcurrentAccess(t *testing.T) {
	c := New(100, 0)
	defer c.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := "key" + string(rune(id*1000+j))
				c.Set(key, "value")
			}
		}(i)
	}

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := "key" + string(rune(id*1000+j))
				c.Get(key)
			}
		}(i)
	}

	// Concurrent stats reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				c.Stats()
				c.Size()
			}
		}()
	}

	// Concurrent deletes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := "key" + string(rune(id*1000+j))
				c.Delete(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify cache is still usable
	c.Set("final", "value")
	val, ok := c.Get("final")
	if !ok || val != "value" {
		t.Error("cache should still work after concurrent access")
	}
}

func TestConcurrentWriteRead(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			c.Set("shared", i)
		}
	}()

	// Reader
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			c.Get("shared")
		}
	}()

	wg.Wait()

	// Cache should still be consistent
	_, ok := c.Get("shared")
	if !ok {
		t.Error("expected shared key to exist")
	}
}

func TestClose(t *testing.T) {
	t.Run("close with ttl", func(t *testing.T) {
		c := New(10, 100*time.Millisecond)
		c.Set("key1", "value1")

		err := c.Close()
		if err != nil {
			t.Errorf("Close returned error: %v", err)
		}

		// Should be safe to call Close multiple times
		err = c.Close()
		if err != nil {
			t.Errorf("second Close returned error: %v", err)
		}
	})

	t.Run("close without ttl", func(t *testing.T) {
		c := New(10, 0)
		c.Set("key1", "value1")

		err := c.Close()
		if err != nil {
			t.Errorf("Close returned error: %v", err)
		}
	})

	t.Run("cache still usable after close", func(t *testing.T) {
		c := New(10, 100*time.Millisecond)
		c.Close()

		c.Set("key1", "value1")
		val, ok := c.Get("key1")
		if !ok || val != "value1" {
			t.Error("cache should still work after close")
		}
	})
}

func TestCapacityEdgeCases(t *testing.T) {
	c := New(1, 0)
	defer c.Close()

	c.Set("key1", "value1")
	if c.Size() != 1 {
		t.Errorf("expected size 1, got %d", c.Size())
	}

	// Should evict key1
	c.Set("key2", "value2")
	if c.Size() != 1 {
		t.Errorf("expected size 1, got %d", c.Size())
	}

	if _, ok := c.Get("key1"); ok {
		t.Error("key1 should have been evicted")
	}

	if _, ok := c.Get("key2"); !ok {
		t.Error("key2 should be present")
	}
}

func TestStatsAccuracy(t *testing.T) {
	c := New(10, 0)
	defer c.Close()

	// Test hit tracking
	c.Set("hit1", "value")
	c.Get("hit1")
	c.Get("hit1")
	stats := c.Stats()
	if stats.Hits != 2 {
		t.Errorf("expected 2 hits, got %d", stats.Hits)
	}

	// Test miss tracking
	c.Get("miss1")
	c.Get("miss2")
	stats = c.Stats()
	if stats.Misses != 2 {
		t.Errorf("expected 2 misses, got %d", stats.Misses)
	}

	// Test expired tracking with TTL
	cWithTTL := New(10, 50*time.Millisecond)
	defer cWithTTL.Close()
	cWithTTL.Set("expire1", "value")
	time.Sleep(100 * time.Millisecond)
	cWithTTL.Get("expire1") // Should count as expired
	stats = cWithTTL.Stats()
	if stats.Expired == 0 {
		t.Error("expected expired count > 0")
	}
}
