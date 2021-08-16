/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.database.bucketselectionstrategy;

import com.arcadedb.database.Document;
import com.arcadedb.schema.DocumentType;

/**
 * Default round-robin implementation that uses all the available buckets. In async mode, it returns the bucket partitioned with the thread id. In this way
 * there is no conflict between documents created by concurrent threads.
 *
 * @author Luca Garulli
 */
public class DefaultBucketSelectionStrategy implements BucketSelectionStrategy {
  protected        int total;
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
      return (int) (Thread.currentThread().getId() % total);

    // COPY THE VALUE ON THE HEAP FOR MULTI-THREAD ACCESS
    int bucketIndex = ++current;
    if (bucketIndex >= total) {
      current = 0;
      bucketIndex = 0;
    }
    return bucketIndex;
  }

  @Override
  public int getBucketIdByKeys(final Object[] keys, final boolean async) {
    // UNSUPPORTED
    return -1;
  }

  @Override
  public String getName() {
    return "round-robin";
  }

  @Override
  public String toString() {
    return getName();
  }
}
