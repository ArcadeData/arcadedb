/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Server-side cache of successful HTTP responses used to make retried non-idempotent (POST) requests
 * safe. The key is NOT the raw client {@code X-Request-Id} header: the caller must build it from the
 * request id combined with method, path, database and body (see
 * {@code AbstractServerHttpHandler.buildIdempotencyKey}) so two unrelated requests that happen to
 * reuse the same correlation id are never conflated. When a client retries the SAME logical request
 * with the same id, the server returns the cached response instead of re-executing the operation,
 * preventing duplicate-key / double-commit violations when the original response was lost in transit.
 * <p>
 * Concurrent identical retries are de-duplicated: the first caller {@link #reserve(String) reserves}
 * the key (installing a PENDING marker) and executes; a concurrent caller observes the marker
 * ({@link Reservation#isInFlight()}) and can wait for the winner's result instead of executing again.
 * <p>
 * Only successful responses (HTTP 2xx) are cached; errors are passed through so the client can retry
 * a fresh call if it chooses. Entries expire after {@link #ttlMs}. The cache is bounded both by the
 * number of entries ({@link #maxEntries}) and by the total cached body bytes ({@link #maxBytes});
 * eviction is O(1) FIFO. Individual responses larger than {@link #maxBodyBytes} are not cached.
 */
public class IdempotencyCache {

  public static final String HEADER_REQUEST_ID = "X-Request-Id";

  public static class CachedEntry {
    public final int    statusCode;
    public final String body;
    public final byte[] binary;
    public final String principal;
    public final long   timestampMs;
    // A PENDING placeholder installed by reserve() while the winning request executes. Not a real
    // cached response: get() never returns it and its body/binary are null.
    final boolean        pending;
    // Released when a pending marker is settled (completed, aborted or evicted) so an in-flight waiter
    // stops blocking. Null for a completed entry.
    final CountDownLatch latch;

    CachedEntry(final int statusCode, final String body, final byte[] binary, final String principal) {
      this(statusCode, body, binary, principal, false, null);
    }

    private CachedEntry(final int statusCode, final String body, final byte[] binary, final String principal,
        final boolean pending, final CountDownLatch latch) {
      this.statusCode = statusCode;
      this.body = body;
      this.binary = binary;
      this.principal = principal;
      this.timestampMs = System.currentTimeMillis();
      this.pending = pending;
      this.latch = latch;
    }

    static CachedEntry newPending() {
      return new CachedEntry(0, null, null, null, true, new CountDownLatch(1));
    }

    long sizeInBytes() {
      long n = 0;
      if (body != null)
        n += body.length();
      if (binary != null)
        n += binary.length;
      return n;
    }

    /**
     * Blocks up to {@code timeoutMs} for a PENDING marker to be settled by the winning request.
     * Returns true if it was settled within the timeout. Always true (no wait) for a completed entry.
     */
    public boolean await(final long timeoutMs) {
      if (latch == null)
        return true;
      try {
        return latch.await(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  /**
   * Outcome of {@link #reserve(String)}. At most one of the three predicates is true.
   */
  public static final class Reservation {
    private final CachedEntry hit;      // non-null when a completed response is already cached
    private final CachedEntry inFlight; // non-null when another request is currently executing
    private final boolean     reserved; // true when THIS caller now owns execution

    private Reservation(final CachedEntry hit, final CachedEntry inFlight, final boolean reserved) {
      this.hit = hit;
      this.inFlight = inFlight;
      this.reserved = reserved;
    }

    /** A completed response is cached and ready to replay. */
    public boolean isHit() {
      return hit != null;
    }

    /** Another request holds the reservation and is executing; the caller may {@code entry().await(..)}. */
    public boolean isInFlight() {
      return inFlight != null;
    }

    /** This caller now owns execution and must later call {@code complete} or {@code abort}. */
    public boolean isReserved() {
      return reserved;
    }

    /** The cached entry (HIT) or the pending marker to await (IN_FLIGHT); null when RESERVED. */
    public CachedEntry entry() {
      return hit != null ? hit : inFlight;
    }
  }

  private final LinkedHashMap<String, CachedEntry> cache = new LinkedHashMap<>();
  private final long                               ttlMs;
  private final int                                maxEntries;
  private final long                               maxBytes;
  private final long                               maxBodyBytes;
  private       long                               currentBytes;

  public IdempotencyCache(final long ttlMs, final int maxEntries) {
    // Backward-compatible defaults: byte bounds effectively disabled so behavior for callers that only
    // cared about entry count is unchanged.
    this(ttlMs, maxEntries, Long.MAX_VALUE, Long.MAX_VALUE);
  }

  public IdempotencyCache(final long ttlMs, final int maxEntries, final long maxBytes, final long maxBodyBytes) {
    this.ttlMs = ttlMs;
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
    this.maxBodyBytes = maxBodyBytes;
  }

  /**
   * Returns the cached completed entry for {@code key} or {@code null} if absent, still pending, or
   * expired. Expired entries are removed as a side effect so callers don't keep paying for their TTL
   * check.
   */
  public synchronized CachedEntry get(final String key) {
    if (key == null || key.isEmpty())
      return null;
    final CachedEntry e = cache.get(key);
    if (e == null)
      return null;
    if (isExpired(e)) {
      removeAndRelease(key);
      return null;
    }
    if (e.pending)
      return null;
    return e;
  }

  /**
   * Atomically looks up {@code key} and, when no live entry exists, installs a PENDING marker owned by
   * the caller. See {@link Reservation}.
   */
  public synchronized Reservation reserve(final String key) {
    if (key == null || key.isEmpty())
      // Not cacheable: let the caller execute without owning a reservation.
      return new Reservation(null, null, false);

    final CachedEntry existing = cache.get(key);
    if (existing != null && !isExpired(existing)) {
      if (existing.pending)
        return new Reservation(null, existing, false);
      return new Reservation(existing, null, false);
    }
    if (existing != null)
      removeAndRelease(key);

    cache.put(key, CachedEntry.newPending());
    return new Reservation(null, null, true);
  }

  /**
   * Settles a reservation previously obtained from {@link #reserve(String)} with the final response.
   * Caches it only when {@code statusCode} is 2xx and the body is within {@link #maxBodyBytes};
   * otherwise the marker is simply cleared. In-flight waiters are released either way.
   */
  public synchronized void complete(final String key, final int statusCode, final String body, final byte[] binary,
      final String principal) {
    if (key == null || key.isEmpty())
      return;
    final CachedEntry current = cache.get(key);
    // Only the marker we installed may be settled; if it is gone (evicted/expired) do nothing.
    if (current == null || !current.pending)
      return;

    final boolean cacheable = statusCode >= 200 && statusCode < 300;
    final CachedEntry completed = cacheable ? new CachedEntry(statusCode, body, binary, principal) : null;
    if (completed != null && completed.sizeInBytes() <= maxBodyBytes)
      store(key, completed);
    else
      removeAndRelease(key);

    // Release any waiter blocked on this marker; it will re-read the completed entry (or find none).
    current.latch.countDown();
  }

  /**
   * Clears a reservation without caching anything (execution failed or produced a non-cacheable
   * result). Releases in-flight waiters so they fall back to executing themselves.
   */
  public synchronized void abort(final String key) {
    if (key == null || key.isEmpty())
      return;
    final CachedEntry current = cache.get(key);
    if (current != null && current.pending)
      removeAndRelease(key);
  }

  /**
   * Caches a successful response directly (no reservation). {@code statusCode} must be 2xx; non-2xx
   * responses are ignored. The {@code principal} is stored so that a later replay can verify the caller
   * matches and a different user cannot replay a cached response merely by guessing a key.
   */
  public synchronized void putSuccess(final String key, final int statusCode, final String body, final byte[] binary,
      final String principal) {
    if (key == null || key.isEmpty())
      return;
    if (statusCode < 200 || statusCode >= 300)
      return;
    final CachedEntry entry = new CachedEntry(statusCode, body, binary, principal);
    if (entry.sizeInBytes() > maxBodyBytes)
      return;
    store(key, entry);
  }

  /**
   * Periodic maintenance: drops entries older than the TTL. Called by the server's scheduler so the
   * cache doesn't grow unboundedly on workloads with low retry rate.
   */
  public synchronized void cleanupExpired() {
    final long cutoff = System.currentTimeMillis() - ttlMs;
    final Iterator<Map.Entry<String, CachedEntry>> it = cache.entrySet().iterator();
    while (it.hasNext()) {
      final CachedEntry entry = it.next().getValue();
      // Insertion order equals ascending timestamp (store() always re-appends with a fresh timestamp), so
      // the first non-expired entry means every later entry is newer too: stop scanning immediately.
      if (entry.timestampMs >= cutoff)
        break;
      it.remove();
      currentBytes -= entry.sizeInBytes();
      if (entry.latch != null)
        entry.latch.countDown();
    }
  }

  public synchronized int size() {
    return cache.size();
  }

  public synchronized long bytes() {
    return currentBytes;
  }

  private boolean isExpired(final CachedEntry e) {
    return System.currentTimeMillis() - e.timestampMs > ttlMs;
  }

  // Inserts (or replaces) an entry and enforces both bounds with O(1) FIFO eviction. Callers hold the
  // monitor.
  private void store(final String key, final CachedEntry entry) {
    removeAndRelease(key);
    cache.put(key, entry);
    currentBytes += entry.sizeInBytes();
    evictToBounds();
  }

  // Drops eldest entries (insertion order) until both bounds are satisfied. Each removal is O(1) via the
  // LinkedHashMap iterator, so a single insert never scans the whole map.
  private void evictToBounds() {
    while (cache.size() > maxEntries || currentBytes > maxBytes) {
      final Iterator<Map.Entry<String, CachedEntry>> it = cache.entrySet().iterator();
      if (!it.hasNext())
        break;
      final CachedEntry evicted = it.next().getValue();
      it.remove();
      currentBytes -= evicted.sizeInBytes();
      if (evicted.latch != null)
        evicted.latch.countDown();
    }
  }

  private void removeAndRelease(final String key) {
    final CachedEntry removed = cache.remove(key);
    if (removed != null) {
      currentBytes -= removed.sizeInBytes();
      if (removed.latch != null)
        removed.latch.countDown();
    }
  }
}
