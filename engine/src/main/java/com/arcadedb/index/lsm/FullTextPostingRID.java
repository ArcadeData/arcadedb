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
package com.arcadedb.index.lsm;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseRID;

/**
 * {@link com.arcadedb.database.RID} variant that carries two extra per-posting statistics used by full-text BM25 scoring: the
 * term frequency ({@link #tf}, how many times the indexed term occurs in the document) and the document length
 * ({@link #docLength}, total number of analyzed tokens in the document).
 * <p>
 * The whole index/transaction pipeline is RID-typed (postings, transaction staging, compaction, page cursors). By extending
 * {@link DatabaseRID} this class rides through that pipeline unchanged: every {@code (RID)} cast, deletion-marker bucket-sign
 * check and {@code RidHashSet} membership test keeps working because {@code equals}/{@code hashCode} are inherited from
 * {@link com.arcadedb.database.RID} and compare only bucket id and offset. Only the value (de)serialization in
 * {@link LSMTreeIndexAbstract} - active when the index has {@code storeTermFrequency} enabled - reads and writes the two extra
 * fields, and only the full-text index interprets them.
 * <p>
 * It lives in {@code com.arcadedb.index.lsm} (rather than {@code ...index.fulltext}) on purpose: the value (de)serialization that
 * produces and consumes it is in {@link LSMTreeIndexAbstract}, so it must be visible at the LSM storage layer. {@code final} so a
 * subclass cannot pass the pervasive {@code instanceof FullTextPostingRID} checks while carrying different statistics.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FullTextPostingRID extends DatabaseRID {
  private final int tf;
  private final int docLength;

  public FullTextPostingRID(final BasicDatabase database, final int bucketId, final long offset, final int tf, final int docLength) {
    super(database, bucketId, offset);
    // Guard against corrupt statistics: a negative tf or docLength would silently produce a nonsensical BM25 score (e.g. a
    // negative or infinite term contribution), so fail fast at construction instead.
    if (tf < 0 || docLength < 0)
      throw new IllegalArgumentException("tf and docLength must be non-negative: tf=" + tf + ", docLength=" + docLength);
    this.tf = tf;
    this.docLength = docLength;
  }

  /** Term frequency: how many times the indexed term occurs in the document. */
  public int getTf() {
    return tf;
  }

  /** Document length: total number of analyzed tokens in the document. */
  public int getDocLength() {
    return docLength;
  }

  /** Includes tf/docLength on top of the RID so serialization/scoring issues are easier to spot in logs and debuggers. */
  @Override
  public String toString() {
    return super.toString() + "{tf=" + tf + ",docLength=" + docLength + "}";
  }
}
