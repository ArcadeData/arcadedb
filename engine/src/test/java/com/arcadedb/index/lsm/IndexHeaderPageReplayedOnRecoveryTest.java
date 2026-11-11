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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A crash right after an index is created can leave its file empty: the header page is committed to the WAL but the
 * asynchronous flush never wrote it. The schema is loaded BEFORE the WAL replay, so that load found a file with no header
 * page and left the index out (#10046), and the replay that then wrote the header did not bring it back: the open served
 * the type with no index - a UNIQUE one enforcing nothing - until the next open. The recovering open must serve the index
 * the replay completed.
 */
class IndexHeaderPageReplayedOnRecoveryTest extends TestHelper {
  private static final String TYPE = "IndexHeaderReplayed";

  @Test
  void anIndexWhoseHeaderPageOnlyTheWalHoldsIsServedAfterRecovery() throws Exception {
    database.getSchema().createDocumentType(TYPE).createProperty("id", Integer.class);
    database.getSchema().getType(TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(TYPE).set("id", i).save();
    });

    final IndexInternal bucketIndex = database.getSchema().getType(TYPE).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
    final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();
    final File mutableFile = new File(((DatabaseInternal) database).getFileManager().getFile(mutable.getFileId()).getFilePath());

    ((DatabaseInternal) database).kill();
    database.close();
    assertThat(new File(getDatabasePath(), "database.lck")).exists();

    // THE FLUSH NEVER REACHED THE INDEX FILE: ONLY THE WAL HOLDS ITS PAGES
    try (final RandomAccessFile raf = new RandomAccessFile(mutableFile, "rw")) {
      raf.setLength(0);
    }

    database = factory.open();

    assertThat(mutableFile.length()).isGreaterThan(0);
    assertThat(((DatabaseInternal) database).getSchema().getEmbedded().getUnavailableUniqueIndexNames()).isEmpty();
    assertThat(database.getSchema().getType(TYPE).getIndexesByProperties("id")).isNotEmpty();
    assertThat(database.lookupByKey(TYPE, "id", 42).hasNext()).isTrue();
    assertThat(database.countType(TYPE, false)).isEqualTo(100);

    // THE UNIQUE CONSTRAINT IS ENFORCED AGAIN
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE).set("id", 7).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }
}
