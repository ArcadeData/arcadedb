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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12358: an LSM index instance whose file has been retired - an HA replica applying a replicated compaction
 * while a query still holds the previous schema generation - reported the read as {@code IllegalArgumentException: File with
 * id N was not found}, which a client takes for a bad request. It is an {@link IndexException}: the index was replaced, as
 * for one dropped or rebuilt under a statement, and a SQL statement is planned again on the current generation.
 */
class RetiredIndexFileReadTest extends TestHelper {

  @Test
  void aReadOfAnIndexWhoseFileWasRetiredIsAnIndexException() throws IOException {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create().createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Doc", "name");
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Doc").set("name", "name-" + (i % 50)).save();
    });

    final LSMTreeIndex index = (LSMTreeIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[name]")).getIndexesOnBuckets()[0];
    final int fileId = index.getFileId();

    // What a replica does with the files a replicated compaction retires, while a query still holds this instance
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getPageManager().deleteFile(db, fileId);
    db.getFileManager().dropFile(fileId);
    try {
      assertThatThrownBy(() -> index.get(new Object[] { "name-7" })).isInstanceOf(IndexException.class)
          .hasMessageContaining("Doc").hasMessageContaining("retired")
          // what every wire answers: retry, not the bad request of the IllegalArgumentException it carries as its cause
          .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.RETRY));
      assertThatThrownBy(() -> index.iterator(true)).isInstanceOf(IndexException.class);
      assertThatThrownBy(() -> index.range(true, new Object[] { "name-1" }, true, new Object[] { "name-3" }, true))
          .isInstanceOf(IndexException.class);
    } finally {
      database.getSchema().getEmbedded().removeFile(fileId);
    }
  }

  /**
   * A scan created before the file was retired loads its pages lazily, so it meets the retired file while it is consumed, not
   * when it is created: the same {@link IndexException}, not the {@code IllegalArgumentException} of the missing file.
   */
  @Test
  void aScanThatOutlivesTheFileOfItsIndexEndsWithAnIndexException() throws IOException, InterruptedException {
    // No automatic compaction: the test compacts once, by itself, before it retires the compacted file
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create().createProperty("name", Type.STRING);
      // Small pages, so the compacted file the scan walks has pages left to load after the scan was created
      database.getSchema().buildTypeIndex("Doc", new String[] { "name" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
          .withPageSize(4096).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < 5_000; i++)
        database.newDocument("Doc").set("name", "name-" + i).save();
    });

    final LSMTreeIndex index = (LSMTreeIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[name]")).getIndexesOnBuckets()[0];
    // The pages of a mutable file are all loaded when a scan is created; those of a compacted one, one at a time as it goes
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).isTrue();
    final LSMTreeIndexCompacted compacted = index.mutable.getSubIndex();
    assertThat(compacted).as("the compaction must have produced a compacted file").isNotNull();
    assertThat(compacted.getTotalPages()).as("the scan must have pages to load after its creation").isGreaterThan(2);
    final int fileId = compacted.getFileId();

    final IndexCursor ascending = index.iterator(true);
    final IndexCursor range = index.range(true, new Object[] { "name-0" }, true, new Object[] { "name-9999" }, true);
    assertThat(ascending.hasNext()).isTrue();
    assertThat(range.hasNext()).isTrue();

    final DatabaseInternal db = (DatabaseInternal) database;
    db.getPageManager().deleteFile(db, fileId);
    db.getFileManager().dropFile(fileId);
    try {
      for (final IndexCursor cursor : new IndexCursor[] { ascending, range })
        assertThatThrownBy(() -> {
          while (cursor.hasNext())
            cursor.next();
        }).isInstanceOf(IndexException.class).hasMessageContaining("retired")
            .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.RETRY));
    } finally {
      database.getSchema().getEmbedded().removeFile(fileId);
    }
  }

  /**
   * The test retires the file of an index the schema still lists, which only a replica applying a compaction does, and only
   * until its schema reload publishes the generation without it: the check would rightly report that index unreadable.
   */
  @Override
  protected void checkDatabaseIntegrity() {
  }
}
