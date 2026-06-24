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
package com.arcadedb.index;

import com.arcadedb.database.Identifiable;

import java.util.Arrays;
import java.util.Objects;

/**
 * One entry of an index lookup result: the matched record, the keys it was found under, and a relevance score.
 * <p>
 * NOTE: {@link #equals(Object)}/{@link #hashCode()} intentionally use only {@code (record, keys)} - the score is excluded. This
 * changed in the BM25 work (issue #4687): a previous version included the score, which meant two entries for the same document
 * with different (float) scores were not deduplicated in a {@code Set}. Code that relied on score being part of identity (none
 * known in the codebase) would see a behavior change.
 */
public class IndexCursorEntry {
  public final Object[]     keys;
  public final Identifiable record;
  /**
   * Integer relevance score. For float-scored entries (e.g. full-text BM25) this is {@link #floatScore} rounded and therefore
   * lossy: read {@link #floatScore} (or {@link IndexCursor#getFloatScore()}) when precision matters. It remains an exact value
   * for indexes that score with integers (e.g. CLASSIC full-text coordination, regular LSM indexes).
   */
  public final int          score;
  /** Full-precision relevance score; equals {@link #score} for integer-scored entries. */
  public final float        floatScore;

  public IndexCursorEntry(final Object[] keys, final Identifiable record, final int score) {
    this.keys = keys;
    this.record = record;
    this.score = score;
    this.floatScore = score;
  }

  public IndexCursorEntry(final Object[] keys, final Identifiable record, final float floatScore) {
    this.keys = keys;
    this.record = record;
    this.score = Math.round(floatScore);
    this.floatScore = floatScore;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final IndexCursorEntry that = (IndexCursorEntry) o;
    // Identity is (record, keys) only. The relevance score is a derived value, not part of identity: including it would let the
    // same document appear twice in a result Set when scored differently (e.g. distinct BM25 floats for the same RID).
    return Objects.equals(record, that.record) && Arrays.equals(keys, that.keys);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(record);
    result = 31 * result + Arrays.hashCode(keys);
    return result;
  }
}
