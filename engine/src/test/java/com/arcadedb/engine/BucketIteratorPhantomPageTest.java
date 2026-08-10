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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #6014: {@code BucketIterator.fetchNext()}'s forward-scan termination check used
 * {@code nextPageNumber > totalPages} instead of {@code >=}, so a full forward iteration went on to fetch one
 * page past the bucket's valid range. {@code TransactionContext.getPage()} delegates that out-of-range fetch to
 * {@code PageManager.getImmutablePage(..., createIfNotExists=true)}, which silently synthesizes a blank page for
 * a {@link PageId} that does not correspond to any real page and caches it in {@code PageManager}'s global read
 * cache.
 */
class BucketIteratorPhantomPageTest extends TestHelper {

  @Test
  void forwardIterationDoesNotCachePhantomPageAfterLastPage() {
    final DocumentType type = database.getSchema().createDocumentType("PhantomPageTest", 1);

    database.begin();
    database.newDocument("PhantomPageTest").set("id", 1).save();
    database.commit();

    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    final int totalPages = bucket.pageCount.get();
    assertThat(totalPages).as("one small record must fit on a single freshly-created page").isEqualTo(1);

    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageId phantomPageId = new PageId((DatabaseInternal) database, bucket.getFileId(), totalPages);

    final Iterator<Record> iterator = bucket.iterator();
    while (iterator.hasNext())
      iterator.next();

    assertThat(pageManager.readCache.containsKey(phantomPageId))
        .as("forward iteration must not synthesize/cache a page past the bucket's valid range (#6014)")
        .isFalse();
  }
}
