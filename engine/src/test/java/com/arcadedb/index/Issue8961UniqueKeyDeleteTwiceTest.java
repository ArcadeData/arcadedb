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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8961: on a UNIQUE index, {@code REMOVE A} then {@code ADD B} collapse into
 * {@code REPLACE B (oldRid = A)}, and the next {@code REMOVE B} used to drop {@code oldRid}, so nothing removed A's
 * committed entry and a following {@code ADD C} failed the uniqueness check against the deleted A.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8961UniqueKeyDeleteTwiceTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE PROPERTY Item.code STRING");
    database.command("sql", "CREATE INDEX ON Item (code) UNIQUE");
    database.transaction(() -> {
      database.newDocument("Item").set("code", "x").save();
      database.newDocument("Item").set("code", "y").save();
    });
  }

  @Test
  void deleteInsertDeleteInsertSameKeyCommits() {
    database.transaction(() -> {
      database.query("sql", "SELECT FROM Item WHERE code = 'x'").next().getRecord().get().asDocument().delete();
      final MutableDocument b = database.newDocument("Item").set("code", "x").save();
      b.delete();
      database.newDocument("Item").set("code", "x").save();
    });

    assertThat(database.query("sql", "SELECT FROM Item WHERE code = 'x'").stream().count()).isEqualTo(1);
    final IndexCursor cursor = database.getSchema().getIndexByName("Item[code]").get(new Object[] { "x" });
    assertThat(cursor.estimateSize()).isEqualTo(1);
    assertThat(database.existsRecord(cursor.next().getIdentity())).isTrue();
  }

  @Test
  void deleteInsertDeleteLeavesNoIndexEntry() {
    database.transaction(() -> {
      database.query("sql", "SELECT FROM Item WHERE code = 'y'").next().getRecord().get().asDocument().delete();
      final MutableDocument b = database.newDocument("Item").set("code", "y").save();
      b.delete();
    });

    final IndexCursor cursor = database.getSchema().getIndexByName("Item[code]").get(new Object[] { "y" });
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      assertThat(database.existsRecord(rid)).as("stale index entry " + rid).isTrue();
    }
    assertThat(database.getSchema().getIndexByName("Item[code]").get(new Object[] { "y" }).estimateSize()).isZero();
  }
}
