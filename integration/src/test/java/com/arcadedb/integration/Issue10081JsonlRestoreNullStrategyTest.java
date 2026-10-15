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
package com.arcadedb.integration;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10081, the restore half: {@code IMPORT DATABASE} re-applies the exported null strategy with
 * {@code TypeIndex.setNullStrategy()}, which used to reach the first bucket's index only. The restored index then reported the
 * exported strategy while the other buckets kept the default one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10081JsonlRestoreNullStrategyTest {
  private static final String SOURCE_PATH  = "target/databases/issue10081-source";
  private static final String RESTORE_PATH = "target/databases/issue10081-restore";
  private static final String FILE         = "target/issue10081-export.jsonl.tgz";
  private static final String TYPE_NAME    = "V10081";
  private static final int    BUCKETS      = 4;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(RESTORE_PATH));
    FileUtils.deleteRecursively(new File(FILE));
  }

  @Test
  void restoredTypeIndexEnforcesTheExportedNullStrategyOnEveryBucket() throws Exception {
    try (final Database db = new DatabaseFactory(SOURCE_PATH).create()) {
      db.transaction(() -> {
        final DocumentType type = db.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
        type.createProperty("k", Type.INTEGER);
        db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
            .withNullStrategy(NULL_STRATEGY.ERROR).create();
      });
      assertThat(((TypeIndex) db.getSchema().getIndexByName(TYPE_NAME + "[k]")).getIndexesOnBuckets()).hasSize(BUCKETS);
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE)).exists();

    try (final Database db = new DatabaseFactory(RESTORE_PATH).create()) {
      // the import creates each type with the destination's default bucket count
      db.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, BUCKETS);
      db.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database db = new DatabaseFactory(RESTORE_PATH).open()) {
      final TypeIndex restored = (TypeIndex) db.getSchema().getIndexByName(TYPE_NAME + "[k]");
      assertThat(restored.getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);

      final IndexInternal[] onBuckets = restored.getIndexesOnBuckets();
      assertThat(onBuckets).hasSize(BUCKETS);
      for (final IndexInternal onBucket : onBuckets)
        assertThat(onBucket.getNullStrategy()).as("null strategy of %s", onBucket.getName()).isEqualTo(NULL_STRATEGY.ERROR);

      // whichever bucket the record lands in, the null key is refused
      for (final Bucket bucket : db.getSchema().getType(TYPE_NAME).getBuckets(false))
        assertThatThrownBy(() -> db.transaction(() -> db.newDocument(TYPE_NAME).save(bucket.getName())))
            .as("a null key must be refused on bucket %s", bucket.getName()).rootCause().hasMessageContaining("cannot be NULL");
    }
  }
}
