/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import com.arcadedb.index.lsm.LSMTreeIndexPostings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The postings the limited search of one fetch read for a disjunction of terms, with the corpus statistics it scored them with,
 * so the complete search that continues the same fetch, when a filter rejected the rows the limit covered, scores them again
 * without reading them again. The statistics travel with the postings so that the continuation goes on in the very order the
 * limited search served its first rows in, even when a commit moved the corpus between the two. A fetch holds its own instance and
 * never shares it with another statement. Each read is handed over once and then dropped, so the postings are held at most until
 * the continuation takes them, or until the fetch ends.
 * <p>
 * Memory: the postings are held by reference, about 20 bytes per posting of every query term in every bucket, from the limited
 * search until then. That is why a fetch asks for an instance only when a filter follows it (the continuation is then expected),
 * and why only a limited search leaves its postings here.
 * <p>
 * The key compares the bucket indexes by identity (no index overrides {@code equals}), so a read is never handed to another index.
 */
public final class FullTextPostingsReuse {
  // Test-only: searches that took their postings from the limited search of their fetch. Nothing in production reads it.
  private static final AtomicLong reused = new AtomicLong();

  private final Map<Key, Read> reads = new HashMap<>();

  public static long getReusedReadsForTesting() {
    return reused.get();
  }

  /** What a limited search read: the postings of each token in each bucket, and the corpus statistics it scored them with. */
  record Read(LSMTreeIndexPostings[][] postings, BM25ScoringContext context) {
  }

  private record Key(List<LSMTreeFullTextIndex> bucketIndexes, List<String> tokens) {
  }

  Read get(final List<LSMTreeFullTextIndex> bucketIndexes, final String[] tokens) {
    final Read read = reads.remove(new Key(List.copyOf(bucketIndexes), List.of(tokens)));
    if (read != null)
      reused.incrementAndGet();
    return read;
  }

  void put(final List<LSMTreeFullTextIndex> bucketIndexes, final String[] tokens, final Read read) {
    reads.put(new Key(List.copyOf(bucketIndexes), List.of(tokens)), read);
  }
}
