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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8983: a key change on an indexed property was written to a HASH index twice at commit - once by the flush of the
 * deferred update (which re-indexed the record against the committed buffer, outside any transaction index queue) and
 * once by the replay of the queue that {@code save()} had already filled. The duplicate write bypassed the unique check
 * and left a double entry for non-unique indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8983HashIndexKeyChangeTest extends TestHelper {

  private String create(final String type, final String kind) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE INDEX ON " + type + " (k) " + kind);
    return type;
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void uniqueKeyChangeToExistingValueIsRefused() {
    for (final String kind : new String[] { "UNIQUE", "UNIQUE_HASH" }) {
      final String t = create("A_" + kind, kind);
      database.transaction(() -> {
        database.newDocument(t).set("k", 1).save();
        database.newDocument(t).set("k", 2).save();
      });

      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE " + t + " SET k = 2 WHERE k = 1")))
          .as(kind).isInstanceOf(DuplicatedKeyException.class);

      assertThat(count("SELECT count(*) AS c FROM " + t + " WHERE k = 2")).as(kind).isEqualTo(1);
      assertThat(count("SELECT count(*) AS c FROM " + t + " WHERE k = 1")).as(kind).isEqualTo(1);
    }
  }

  @Test
  void keyChangeWritesOneIndexEntry() {
    for (final String kind : new String[] { "UNIQUE", "NOTUNIQUE", "UNIQUE_HASH", "NOTUNIQUE_HASH" }) {
      final String t = create("C_" + kind, kind);
      database.transaction(() -> database.newDocument(t).set("k", 0).save());
      database.transaction(() -> database.command("sql", "UPDATE " + t + " SET k = 1 WHERE k = 0"));
      assertThat(database.getSchema().getType(t).getPolymorphicIndexByProperties("k").countEntries()).as(kind).isEqualTo(1);
    }
  }

  @Test
  void deletedRecordLeavesNoStaleEntryAfterKeyChange() {
    for (final String kind : new String[] { "NOTUNIQUE", "NOTUNIQUE_HASH" }) {
      final String t = create("B_" + kind, kind);
      final RID[] rids = new RID[2];
      database.transaction(() -> rids[0] = database.newDocument(t).set("k", 0).save().getIdentity());
      database.transaction(() -> database.command("sql", "UPDATE " + t + " SET k = 1 WHERE k = 0"));
      database.transaction(() -> database.command("sql", "DELETE FROM " + t + " WHERE k = 1"));
      database.transaction(() -> rids[1] = database.newDocument(t).set("k", 5).save().getIdentity());

      assertThat(count("SELECT count(*) AS c FROM " + t + " WHERE k = 1")).as(kind).isZero();
      assertThat(database.getSchema().getType(t).getPolymorphicIndexByProperties("k").countEntries()).as(kind).isEqualTo(1);
    }
  }

  @Test
  void javaApiKeyChangeOnUniqueHashIsRefused() {
    final String t = create("D", "UNIQUE_HASH");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      rid[0] = database.newDocument(t).set("k", 1).save().getIdentity();
      database.newDocument(t).set("k", 2).save();
    });
    assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("k", 2).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void sameRecordUpdatedTwiceInOneTransactionKeepsOneEntry() {
    for (final String kind : new String[] { "UNIQUE", "NOTUNIQUE", "UNIQUE_HASH", "NOTUNIQUE_HASH" }) {
      final String t = create("E_" + kind, kind);
      database.transaction(() -> database.newDocument(t).set("k", 0).save());
      database.transaction(() -> {
        database.command("sql", "UPDATE " + t + " SET k = 1 WHERE k = 0");
        database.command("sql", "UPDATE " + t + " SET k = 2 WHERE k = 1");
      });
      assertThat(database.getSchema().getType(t).getPolymorphicIndexByProperties("k").countEntries()).as(kind).isEqualTo(1);
      assertThat(count("SELECT count(*) AS c FROM " + t + " WHERE k = 2")).as(kind).isEqualTo(1);
      assertThat(count("SELECT count(*) AS c FROM " + t + " WHERE k = 1")).as(kind).isZero();
    }
  }

  @Test
  void multiPageDocumentWithIndexedPropertyKeepsOneEntry() {
    database.command("sql", "CREATE DOCUMENT TYPE Big");
    database.command("sql", "CREATE PROPERTY Big.k INTEGER");
    database.command("sql", "CREATE INDEX ON Big (k) UNIQUE_HASH");
    database.transaction(() -> database.newDocument("Big").set("k", 0).set("s", "x".repeat(200_000)).save());
    database.transaction(() -> database.command("sql", "UPDATE Big SET k = 1 WHERE k = 0"));
    assertThat(database.getSchema().getType("Big").getPolymorphicIndexByProperties("k").countEntries()).isEqualTo(1);
    assertThat(count("SELECT count(*) AS c FROM Big WHERE k = 1")).isEqualTo(1);
  }

  @Test
  void recordsSwappingUniqueKeysInOneTransactionKeepEveryEntry() {
    // The ADD of a key is queued before the REMOVE of the record that held it: the REMOVE must not displace the ADD
    for (final String kind : new String[] { "UNIQUE", "UNIQUE_HASH" }) {
      final String t = create("S_" + kind, kind);
      database.command("sql", "CREATE PROPERTY " + t + ".n INTEGER");
      database.transaction(() -> {
        for (int i = 0; i < 5; i++)
          database.newDocument(t).set("k", i).set("n", i).save();
      });
      database.transaction(() -> {
        final List<MutableDocument> docs = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + t)) {
          while (rs.hasNext())
            docs.add(rs.next().getRecord().orElseThrow().asDocument().modify());
        }
        for (final MutableDocument d : docs)
          d.set("k", 4 - d.getInteger("n")).save();
      });
      assertThat(database.getSchema().getType(t).getPolymorphicIndexByProperties("k").countEntries()).as(kind).isEqualTo(5);
      for (int i = 0; i < 5; i++)
        try (final ResultSet rs = database.query("sql", "SELECT n FROM " + t + " WHERE k = " + i)) {
          assertThat(rs.next().<Integer>getProperty("n")).as(kind + " k=" + i).isEqualTo(4 - i);
        }
    }
  }
}
