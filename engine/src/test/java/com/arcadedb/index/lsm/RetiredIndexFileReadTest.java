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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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
          .hasMessageContaining("Doc").hasMessageContaining("retired");
      assertThatThrownBy(() -> index.iterator(true)).isInstanceOf(IndexException.class);
      assertThatThrownBy(() -> index.range(true, new Object[] { "name-1" }, true, new Object[] { "name-3" }, true))
          .isInstanceOf(IndexException.class);
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
