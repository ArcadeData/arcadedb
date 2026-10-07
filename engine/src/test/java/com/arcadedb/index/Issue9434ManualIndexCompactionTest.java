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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9434: a MANUAL LSM index (no type) lost the name its creator gave it after a compaction + restart, because the
 * compaction named the new file after the old one's prefix up to the last underscore and the schema had no entry to
 * restore the name from; a name with no underscore made the compaction throw.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9434ManualIndexCompactionTest extends TestHelper {
  private static final int PAGE_SIZE = 8192;
  private static final int RECORDS   = 700;

  @Test
  void nameWithUnderscoreSurvivesCompactionAndRestart() throws Exception {
    assertNameSurvives("Manual_Idx");
  }

  @Test
  void nameWithoutUnderscoreCompactsAndSurvivesRestart() throws Exception {
    assertNameSurvives("ManualIdx");
  }

  @Test
  void nameEndingWithDigitsSurvivesCompactionAndRestart() throws Exception {
    assertNameSurvives("Manual_123");
  }

  @Test
  void secondCompactionAfterRestartKeepsTheName() throws Exception {
    final List<String> keys = fill("Manual_Idx");
    compact("Manual_Idx");
    reopenDatabase();
    insert("Manual_Idx", keys);
    compact("Manual_Idx");
    reopenDatabase();

    assertThat(database.getSchema().existsIndex("Manual_Idx")).isTrue();
    assertServes("Manual_Idx", keys);
  }

  private void assertNameSurvives(final String name) throws Exception {
    final List<String> keys = fill(name);
    final LSMTreeIndex before = (LSMTreeIndex) database.getSchema().getIndexByName(name);
    compact(name);
    assertThat(before.getName()).isEqualTo(name);
    assertThat(before.getMostRecentFileName()).isNotEqualTo(name);

    reopenDatabase();

    assertThat(database.getSchema().existsIndex(name)).as("lookup by the name the user gave it").isTrue();
    final LSMTreeIndex after = (LSMTreeIndex) database.getSchema().getIndexByName(name);
    assertThat(after.getName()).isEqualTo(name);
    assertServes(name, keys);
  }

  private List<String> fill(final String name) {
    database.getSchema().buildManualIndex(name, new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
    final List<String> keys = new ArrayList<>(RECORDS * 2);
    insert(name, keys);
    return keys;
  }

  private void insert(final String name, final List<String> keys) {
    final Index index = database.getSchema().getIndexByName(name);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final String key = UUID.randomUUID().toString();
        keys.add(key);
        index.put(new Object[] { key }, new RID[] { new RID(1, keys.size()) });
      }
    });
  }

  private void compact(final String name) throws Exception {
    final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getIndexByName(name);
    assertThat(index.getMutableIndex().getTotalPages()).as("the compaction needs at least 2 mutable pages to run")
        .isGreaterThanOrEqualTo(2);
    database.async().waitCompletion();
    index.scheduleCompaction();
    assertThat(index.compact()).as("the compaction must actually run").isTrue();
  }

  private void assertServes(final String name, final List<String> keys) {
    final Index index = database.getSchema().getIndexByName(name);
    assertThat(index.countEntries()).isEqualTo(keys.size());
    for (int i = 0; i < keys.size(); i += 97)
      assertThat(index.get(new Object[] { keys.get(i) }).hasNext()).as("key %s", keys.get(i)).isTrue();
  }
}
