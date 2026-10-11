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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9651, engine side: a drop takes the sub-indexes of a type index away one at a time while the index still answers as
 * valid. A cursor opened in that window read the live sub-index list a second time and, finding it emptied, raised a
 * {@link java.util.NoSuchElementException}, which a Gremlin traversal takes for the end of the stream: an empty answer
 * returned as a success. The index now reads one snapshot and reports an emptied index as the stale-index
 * {@link IndexException} every reader handles, is not ready for queries while a drop is under way, and tells a reader
 * whether what it read is still the whole index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9651TypeIndexConcurrentDropReadTest extends TestHelper {

  @Test
  void anIndexWhoseSubIndexesAreGoneIsReportedAsStaleNotAsAnEmptyStream() {
    final TypeIndex index = createIndex();
    final IndexInternal[] snapshot = index.getIndexesOnBuckets();
    assertThat(index.isUnchangedSince(snapshot)).isTrue();

    // the state a concurrent drop leaves for a moment: the wrapper still valid, its sub-indexes gone
    for (final IndexInternal subIndex : snapshot)
      index.removeIndexOnBucket(subIndex);
    try {
      assertThat(index.isValid()).isTrue();
      assertThat(index.isReadyForQueries()).isFalse();
      assertThat(index.isUnchangedSince(snapshot)).isFalse();
      assertThatThrownBy(() -> index.iterator(true, new Object[] { 1L }, true)).isInstanceOf(IndexException.class);
      assertThatThrownBy(() -> index.iterator(true)).isInstanceOf(IndexException.class);
      assertThatThrownBy(() -> index.range(true, new Object[] { 1L }, true, new Object[] { 5L }, true))
          .isInstanceOf(IndexException.class);
    } finally {
      for (final IndexInternal subIndex : snapshot)
        index.addIndexOnBucket(subIndex);
    }
    assertThat(index.isUnchangedSince(snapshot)).isTrue();
  }

  @Test
  void aDroppedIndexIsNoLongerUnchangedAndItsReadsAreStale() {
    final TypeIndex index = createIndex();
    final IndexInternal[] snapshot = index.getIndexesOnBuckets();

    database.getSchema().dropIndex(index.getName());

    assertThat(index.isUnchangedSince(snapshot)).isFalse();
    assertThat(index.isReadyForQueries()).isFalse();
    assertThat(TypeIndex.isStaleRead(index, new IndexException("gone"))).isTrue();
    assertThat(TypeIndex.isStaleRead(index, new IllegalArgumentException("File with id 7 was not found"))).isTrue();
    assertThat(TypeIndex.isStaleRead(index, new IllegalArgumentException("something else"))).isFalse();
  }

  @Test
  void aDropRefusedBeforeTakingAnythingAwayLeavesTheIndexReady() {
    final TypeIndex index = createIndex();
    final IndexInternal subIndex = index.getIndexesOnBuckets()[0];
    // a sub-index busy compacting: the drop cannot acquire it and is refused before removing anything
    assertThat(subIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.AVAILABLE },
        IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS)).isTrue();
    try {
      assertThatThrownBy(index::drop).isInstanceOf(NeedRetryException.class);
    } finally {
      subIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
          IndexInternal.INDEX_STATUS.AVAILABLE);
    }

    assertThat(index.isValid()).isTrue();
    assertThat(index.isReadyForQueries()).isTrue();
    assertThat(index.isUnchangedSince(index.getIndexesOnBuckets())).isTrue();
  }

  private TypeIndex createIndex() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("n", Type.LONG);
    database.transaction(() -> {
      for (long i = 0; i < 10; i++)
        database.newDocument("Doc").set("n", i).save();
    });
    return type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");
  }
}
