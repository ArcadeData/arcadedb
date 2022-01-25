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
import com.arcadedb.schema.DocumentType;

/**
 * Default round-robin implementation that uses all the available buckets. For async execution, the thread selection strategy is used.
 *
 * @author Luca Garulli
 */
public class RoundRobinBucketSelectionStrategy extends ThreadBucketSelectionStrategy {
  private volatile int current = -1;

  @Override
  public void setType(final DocumentType type) {
    this.total = type.getBuckets(false).size();
    if (current >= total)
      // RESET IT
      current = -1;
  }

  @Override
  public int getBucketIdByRecord(final Document record, final boolean async) {
    if (async)
      return super.getBucketIdByRecord(record, async);

    // COPY THE VALUE ON THE HEAP FOR MULTI-THREAD ACCESS
    int bucketIndex = ++current;
    if (bucketIndex >= total) {
      current = 0;
      bucketIndex = 0;
    }
    return bucketIndex;
  }

  @Override
  public String getName() {
    return "round-robin";
  }
}
