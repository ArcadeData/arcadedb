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
package com.arcadedb.database.bucketselectionstrategy;

import com.arcadedb.database.Document;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;

/**
 * Interface to delegate the assignment of the bucket based on document or keys.
 *
 * @author Luca Garulli
 */
public interface BucketSelectionStrategy {
  void setType(LocalDocumentType type);

  int getBucketIdByRecord(Document record, boolean async);

  /**
   * Resolves the bucket that holds the records matching {@code keyValues}, or -1 when this strategy cannot tell.
   * <p>
   * {@code propertyNames} states which properties {@code keyValues} are the values OF, and is what makes the answer
   * verifiable: a record is placed by {@link #getBucketIdByRecord}, which hashes the strategy's OWN properties, so
   * hashing an arbitrary key array only lands on the right bucket when the two property sets are the same one. Issue
   * #5589 came from a caller that passed the keys of an unrelated index; without the names in the signature neither
   * side could detect it, and the lookup silently read a bucket the record was not in.
   * <p>
   * A strategy that cannot verify the match MUST return -1, which callers read as "search every bucket": correct,
   * only slower.
   *
   * @param propertyNames properties {@code keyValues} belong to, in the same order. {@code null} when the caller
   *                      cannot say, which no partitioning strategy may treat as a match.
   * @param keyValues     one value per entry of {@code propertyNames}
   * @param async         whether the caller runs on the async pipeline
   */
  int getBucketIdByKeys(List<String> propertyNames, Object[] keyValues, boolean async);

  /**
   * @deprecated since the key array alone cannot be checked against the partition properties (issue #5589). Use
   * {@link #getBucketIdByKeys(List, Object[], boolean)}. Kept so third-party callers keep compiling; it resolves to
   * the unverifiable case and therefore never prunes.
   */
  @Deprecated
  default int getBucketIdByKeys(final Object[] keyValues, final boolean async) {
    return getBucketIdByKeys(null, keyValues, async);
  }

  String getName();

  default JSONObject toJSON() {
    return new JSONObject().put("name", getName());
  }

  BucketSelectionStrategy copy();
}
