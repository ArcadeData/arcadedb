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
 * The postings the limited search of one fetch read for a disjunction of terms, so the complete search that continues the same
 * fetch, when a filter rejected the rows the limit covered, scores them again without reading them again. A fetch holds its own
 * instance and never shares it with another statement. Each read is handed over once and then dropped, so the postings are held
 * at most until the continuation takes them, or until the fetch ends. The key compares the bucket indexes by identity (no index
 * overrides {@code equals}), so a read is never handed to another index.
 */
public final class FullTextPostingsReuse {
  // Test-only: searches that took their postings from the limited search of their fetch. Nothing in production reads it.
  private static final AtomicLong reused = new AtomicLong();

  private final Map<Key, LSMTreeIndexPostings[][]> reads = new HashMap<>();

  public static long getReusedReadsForTesting() {
    return reused.get();
  }

  private record Key(List<LSMTreeFullTextIndex> bucketIndexes, List<String> tokens) {
  }

  LSMTreeIndexPostings[][] get(final List<LSMTreeFullTextIndex> bucketIndexes, final String[] tokens) {
    final LSMTreeIndexPostings[][] postings = reads.remove(new Key(List.copyOf(bucketIndexes), List.of(tokens)));
    if (postings != null)
      reused.incrementAndGet();
    return postings;
  }

  void put(final List<LSMTreeFullTextIndex> bucketIndexes, final String[] tokens, final LSMTreeIndexPostings[][] postings) {
    reads.put(new Key(List.copyOf(bucketIndexes), List.of(tokens)), postings);
  }
}
