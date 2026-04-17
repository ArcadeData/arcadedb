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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Server-side cache of successful HTTP responses keyed by the client-supplied
 * {@code X-Request-Id} header. When a client retries a non-idempotent request (POST) with the
 * same {@code X-Request-Id}, the server returns the cached response instead of re-executing the
 * operation. This prevents duplicate-key / double-commit violations when the original response
 * was lost in transit but the server had already committed.
 * <p>
 * Only successful responses (HTTP 2xx) are cached; errors are passed through so the client can
 * retry a fresh call if it chooses. Entries expire after {@link #ttlMs} and the cache is bounded
 * by {@link #maxEntries}; on overflow the oldest entry is dropped.
 */
public class IdempotencyCache {

  public static final String HEADER_REQUEST_ID = "X-Request-Id";

  public static class CachedEntry {
    public final int    statusCode;
    public final String body;
    public final byte[] binary;
    public final String principal;
    public final long   timestampMs;

    CachedEntry(final int statusCode, final String body, final byte[] binary, final String principal) {
      this.statusCode = statusCode;
      this.body = body;
      this.binary = binary;
      this.principal = principal;
      this.timestampMs = System.currentTimeMillis();
    }
  }

  private final ConcurrentHashMap<String, CachedEntry> cache = new ConcurrentHashMap<>();
  private final long                                   ttlMs;
  private final int                                    maxEntries;

  public IdempotencyCache(final long ttlMs, final int maxEntries) {
    this.ttlMs = ttlMs;
    this.maxEntries = maxEntries;
  }

  /**
   * Returns the cached entry for {@code requestId} or {@code null} if absent or expired. Expired
   * entries are removed as a side effect so callers don't keep paying for their TTL check.
   */
  public CachedEntry get(final String requestId) {
    if (requestId == null || requestId.isEmpty())
      return null;
    final CachedEntry e = cache.get(requestId);
    if (e == null)
      return null;
    if (System.currentTimeMillis() - e.timestampMs > ttlMs) {
      cache.remove(requestId, e);
      return null;
    }
    return e;
  }

  /**
   * Caches a successful response. {@code statusCode} must be 2xx; non-2xx responses are ignored.
   * The {@code principal} is stored so that a later replay can verify the caller matches and a
   * different user cannot replay a cached response merely by guessing a request id.
   */
  public void putSuccess(final String requestId, final int statusCode, final String body, final byte[] binary,
      final String principal) {
    if (requestId == null || requestId.isEmpty())
      return;
    if (statusCode < 200 || statusCode >= 300)
      return;
    if (cache.size() >= maxEntries)
      evictOldest();
    cache.put(requestId, new CachedEntry(statusCode, body, binary, principal));
  }

  /**
   * Periodic maintenance: drops entries older than the TTL. Called by the server's
   * scheduler so the cache doesn't grow unboundedly on workloads with low retry rate.
   */
  public void cleanupExpired() {
    final long cutoff = System.currentTimeMillis() - ttlMs;
    final Iterator<Map.Entry<String, CachedEntry>> it = cache.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<String, CachedEntry> e = it.next();
      if (e.getValue().timestampMs < cutoff)
        it.remove();
    }
  }

  public int size() {
    return cache.size();
  }

  private void evictOldest() {
    // Not a perfect LRU; scans for the single oldest entry and drops it. The common case at
    // workloads with reasonable retry rates keeps the cache well under maxEntries, so this
    // slow path rarely runs and doesn't need to be micro-optimized.
    String oldestKey = null;
    long oldestTs = Long.MAX_VALUE;
    for (final Map.Entry<String, CachedEntry> e : cache.entrySet()) {
      if (e.getValue().timestampMs < oldestTs) {
        oldestTs = e.getValue().timestampMs;
        oldestKey = e.getKey();
      }
    }
    if (oldestKey != null)
      cache.remove(oldestKey);
  }
}
