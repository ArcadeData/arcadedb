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
package com.arcadedb.bench.workload.rag;

import java.util.SplittableRandom;

/**
 * Deterministic multi-tenant data model of the hybrid-rag workload: every attribute of a chunk or an entity is a pure function of
 * the seed and its id, so the loader and the query side agree without sharing state. Chunks mention 3 distinct entities drawn
 * from a Zipf law (s = 1.1, rank 0 is entity 0, so the hubs are the low ids), and an entity is related to 4 other entities.
 * The Zipf cumulative weights are computed once per instance. Instances are immutable and thread safe.
 */
public final class RagDataset {
  /** Fewest chunks accepted: {@code entities() = chunks / 20} must be at least 5 so {@link #related(int)} can pick 4 distinct others. */
  public static final int MIN_CHUNKS = 100;

  private static final int    TENANTS          = 10;
  private static final int    MENTIONS         = 3;
  private static final int    RELATED          = 4;
  private static final double ZIPF_S           = 1.1;
  private static final long   GOLDEN           = 0x9E3779B97F4A7C15L;
  private static final long   VISIBILITY_SALT   = 0xC2B2AE3D27D4EB4FL;
  private static final long   RELATED_SALT      = 0x165667B19E3779F9L;

  private final long     seed;
  private final int      entities;
  private final double[] zipfCdf;

  public RagDataset(final long seed, final int chunks) {
    if (chunks < MIN_CHUNKS)
      throw new IllegalArgumentException("hybrid-rag needs at least " + MIN_CHUNKS + " chunks, got " + chunks);
    this.seed = seed;
    this.entities = chunks / 20;
    this.zipfCdf = new double[entities];
    double sum = 0;
    for (int i = 0; i < entities; i++) {
      sum += 1.0 / Math.pow(i + 1, ZIPF_S);
      zipfCdf[i] = sum;
    }
    for (int i = 0; i < entities; i++)
      zipfCdf[i] /= sum;
  }

  public int entities() {
    return entities;
  }

  /** The tenant of a chunk, 0..9. */
  public int tenant(final int chunk) {
    return (int) Long.remainderUnsigned(mix(seed + GOLDEN * (chunk + 1L)), TENANTS);
  }

  /** {@code public} 50%, {@code private} 40% or {@code restricted} 10%. */
  public String visibility(final int chunk) {
    final int r = (int) Long.remainderUnsigned(mix((seed ^ VISIBILITY_SALT) + GOLDEN * (chunk + 1L)), 100);
    return r < 50 ? "public" : r < 90 ? "private" : "restricted";
  }

  /** Three distinct entities, Zipf distributed over {@link #entities()}. */
  public int[] mentions(final int chunk) {
    final SplittableRandom rnd = new SplittableRandom(seed ^ chunk);
    final int[] m = new int[MENTIONS];
    int n = 0;
    while (n < MENTIONS) {
      final int e = zipf(rnd.nextDouble());
      boolean dup = false;
      for (int i = 0; i < n; i++)
        if (m[i] == e) {
          dup = true;
          break;
        }
      if (!dup)
        m[n++] = e;
    }
    return m;
  }

  /** Four distinct entities, uniform, never {@code entity} itself. */
  public int[] related(final int entity) {
    final SplittableRandom rnd = new SplittableRandom(seed ^ RELATED_SALT ^ (entity * GOLDEN));
    final int[] r = new int[RELATED];
    int n = 0;
    while (n < RELATED) {
      int e = rnd.nextInt(entities - 1);
      if (e >= entity)
        e++;
      boolean dup = false;
      for (int i = 0; i < n; i++)
        if (r[i] == e) {
          dup = true;
          break;
        }
      if (!dup)
        r[n++] = e;
    }
    return r;
  }

  /** Whether a chunk is visible to a query tenant: public, or private to the same tenant. Restricted is never visible. */
  public static boolean visible(final String visibility, final int chunkTenant, final int queryTenant) {
    return "public".equals(visibility) || ("private".equals(visibility) && chunkTenant == queryTenant);
  }

  private int zipf(final double u) {
    int lo = 0, hi = entities - 1;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (zipfCdf[mid] < u)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  private static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }
}
