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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.vector.LSMVectorIndexMutable;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8849: an index-compaction temporary ({@code *.temp_<ext>}) left in the database directory
 * - by a crash mid-compaction, by a snapshot shipped from a pre-#8019 leader, or by a restored backup - is invisible to
 * the open-time component scan (its extension is not in {@code SUPPORTED_FILE_EXT}), and nothing else removed it, so it
 * occupied disk for the life of the node. A read-write open now deletes it.
 */
class Issue8849OrphanCompactionTemporarySweepTest extends TestHelper {
  private static final String TYPE      = "Issue8849Doc";
  // A TYPE WHOSE NAME STARTS WITH THE TEMPORARY PREFIX: ITS FILES CARRY "temp_" IN THE NAME, NOT IN THE EXTENSION, AND
  // MUST SURVIVE THE SWEEP
  private static final String TEMP_TYPE = "temp_readings";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE).createProperty("id", Integer.class);
    database.getSchema().getType(TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    database.getSchema().createDocumentType(TEMP_TYPE).createProperty("id", Integer.class);
    database.getSchema().getType(TEMP_TYPE).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument(TYPE).set("id", i).save();
        database.newDocument(TEMP_TYPE).set("id", i).save();
      }
    });
  }

  @Test
  void orphanTemporariesAreDeletedOnOpen() throws IOException {
    final File[] orphans = plantOrphans();

    reopenDatabase();

    for (final File orphan : orphans)
      assertThat(orphan).doesNotExist();
    assertDataIntact();
  }

  @Test
  void orphanTemporaryLeftByACrashIsDeletedOnRecovery() throws IOException {
    ((DatabaseInternal) database).kill();
    // DEREGISTER THE KILLED INSTANCE: database.lck STAYS ON DISK, SO THE NEXT OPEN GOES THROUGH RECOVERY
    database.close();
    assertThat(new File(getDatabasePath(), "database.lck")).exists();

    final File[] orphans = plantOrphans();

    database = factory.open();

    for (final File orphan : orphans)
      assertThat(orphan).doesNotExist();
    assertDataIntact();
  }

  @Test
  void readOnlyOpenLeavesTemporariesInPlace() throws IOException {
    final File[] orphans = plantOrphans();

    reopenDatabaseInReadOnlyMode();

    // A READ-ONLY OPEN OWNS NOTHING (IT NEVER TAKES database.lck) AND MUST NOT WRITE TO THE DIRECTORY
    for (final File orphan : orphans)
      assertThat(orphan).exists();
    assertDataIntact();

    reopenDatabase();
    for (final File orphan : orphans)
      assertThat(orphan).doesNotExist();
  }

  @Test
  void filesOfATypeNamedLikeTheTemporaryPrefixAreNotSwept() {
    final File dir = new File(getDatabasePath());
    final String[] before = dir.list((d, name) -> name.startsWith(TEMP_TYPE));
    assertThat(before).isNotEmpty();

    reopenDatabase();

    assertThat(dir.list((d, name) -> name.startsWith(TEMP_TYPE))).containsExactlyInAnyOrder(before);
    assertDataIntact();
  }

  /**
   * Plants one orphan per producer of {@link PaginatedComponent#TEMP_EXT} files - the LSM-tree mutable index and the
   * LSM vector index - named as the producers name them, next to the real files of the database.
   */
  private File[] plantOrphans() throws IOException {
    database.close();

    final File dir = new File(getDatabasePath());
    final File[] orphans = new File[] {
        new File(dir, TYPE + "_0_99999.900.262144.v0." + PaginatedComponent.TEMP_EXT + LSMTreeIndexMutable.UNIQUE_INDEX_EXT),
        new File(dir, TEMP_TYPE + "_0_99999.901.262144.v0." + PaginatedComponent.TEMP_EXT + LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT),
        new File(dir, "vectors_99999.902.65536.v0." + PaginatedComponent.TEMP_EXT + LSMVectorIndexMutable.FILE_EXT) };
    for (final File orphan : orphans) {
      Files.write(orphan.toPath(), new byte[65_536]);
      assertThat(PaginatedComponent.isTemporaryFileName(orphan.getName())).isTrue();
    }
    return orphans;
  }

  private void assertDataIntact() {
    assertThat(database.countType(TYPE, false)).isEqualTo(100);
    assertThat(database.countType(TEMP_TYPE, false)).isEqualTo(100);
    assertThat(database.lookupByKey(TYPE, "id", 42).hasNext()).isTrue();
    assertThat(database.lookupByKey(TEMP_TYPE, "id", 42).hasNext()).isTrue();
  }
}
