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

import java.util.List;

/**
 * Threaded implementation that returns the bucket partitioned with the thread id. In this way there is no conflict between documents created by concurrent threads.
 *
 * @author Luca Garulli
 */
public class ThreadBucketSelectionStrategy implements BucketSelectionStrategy {
  // VOLATILE FOR THE SAME REASON AS LocalDocumentType.bucketSelectionStrategy (ISSUE #7119). PUBLISHING A NEW STRATEGY
  // THROUGH THAT VOLATILE FIELD ALREADY CARRIES THIS WRITE, BUT addBucketInternal()/removeBucket() REBIND THE STRATEGY
  // ALREADY IN PLACE - THEY CALL setType(this) ON THE SAME OBJECT AND MAKE NO VOLATILE WRITE AFTERWARDS - SO WITHOUT
  // THIS A CONCURRENT INSERT COULD KEEP READING THE BUCKET COUNT FROM BEFORE THE GROW OR SHRINK, AND HAND OUT AN INDEX
  // ONE PAST THE LAST BUCKET (THE SHAPE OF ISSUE #6380). ONE ACQUIRING LOAD PER RECORD PLACEMENT, AGAINST A LOOKUP AND
  // AN INSERT.
  protected volatile int total;

  @Override
  public void setType(final LocalDocumentType type) {
    this.total = type.getBuckets(false).size();
  }

  @Override
  public BucketSelectionStrategy copy() {
    final ThreadBucketSelectionStrategy copy = new ThreadBucketSelectionStrategy();
    copy.total = total;
    return copy;
  }

  @Override
  public int getBucketIdByRecord(final Document record, final boolean async) {
    return (int) (Thread.currentThread().threadId() % total);
  }

  @Override
  public int getBucketIdByKeys(final List<String> propertyNames, final Object[] keys, final boolean async) {
    // UNSUPPORTED: PLACEMENT IS BY THREAD ID, SO NO KEY CAN PREDICT THE BUCKET
    return -1;
  }

  @Override
  public String getName() {
    return "thread";
  }

  @Override
  public String toString() {
    return getName();
  }
}
