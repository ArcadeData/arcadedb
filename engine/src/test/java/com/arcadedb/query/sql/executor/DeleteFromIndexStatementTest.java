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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code DELETE FROM INDEX:<name> WHERE key ...} removes entries from an index and answers the number of entries it removed. The
 * expectations come from a model of the index kept by the test: the list of (key, record) pairs the type was loaded with.
 * The statement with a rid condition answered 0 whatever it removed (issue #13018).
 */
class DeleteFromIndexStatementTest extends TestHelper {
  private static final int[] KEYS = { 0, 1, 1, 2, 3, 3, 3, 4, 5, 5, 7, 8, 8, 9 };

  private final List<RID> rids = new ArrayList<>();

  /** The statement removes index entries and leaves their records: the integrity check at the end would judge the fixture. */
  @Override
  protected void endTest() {
    rebuildIndexesFromRecords();
  }

  private String load(final String typeName) {
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.command("sql", "CREATE PROPERTY " + typeName + ".k INTEGER");
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName, "k");
    rids.clear();
    database.transaction(() -> {
      for (int i = 0; i < KEYS.length; i++)
        rids.add(database.newDocument(typeName).set("k", KEYS[i]).set("w", i).save().getIdentity());
    });
    return "INDEX:`" + typeName + "[k]`";
  }

  /** The records the index answers for a key: its entries, read through an indexed lookup. */
  private int indexedRows(final String typeName, final int key) {
    try (final ResultSet rows = database.query("sql", "SELECT count(*) AS c FROM " + typeName + " WHERE k = " + key)) {
      return rows.next().<Number>getProperty("c").intValue();
    }
  }

  /** The records of the type, read by a scan: an index delete leaves them where they are. */
  private long records(final String typeName) {
    try (final ResultSet rows = database.query("sql", "SELECT count(*) AS c FROM " + typeName + " WHERE w >= 0")) {
      return rows.next().<Number>getProperty("c").longValue();
    }
  }

  private long delete(final String statement) {
    final long[] count = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", statement)) {
        count[0] = rs.next().<Number>getProperty("count").longValue();
      }
    });
    return count[0];
  }

  private void assertKeyCondition(final String typeName, final String condition, final IntPredicate matches) {
    final String target = load(typeName);
    long expected = 0;
    for (final int key : KEYS)
      if (matches.test(key))
        expected++;

    assertThat(delete("DELETE FROM " + target + " WHERE " + condition)).as(condition + ": entries removed").isEqualTo(expected);

    for (int key = 0; key <= 9; key++) {
      int remaining = 0;
      for (final int k : KEYS)
        if (k == key && !matches.test(k))
          remaining++;
      assertThat(indexedRows(typeName, key)).as(condition + ": entries left under key " + key).isEqualTo(remaining);
    }
    assertThat(records(typeName)).as(condition + ": the records are not deleted").isEqualTo(KEYS.length);
  }

  @Test
  void anEqualityRemovesEveryEntryOfTheKey() {
    assertKeyCondition("T_eq", "key = 3", k -> k == 3);
  }

  @Test
  void anEqualityOnAKeyWithNoEntryRemovesNothing() {
    assertKeyCondition("T_none", "key = 6", k -> k == 6);
  }

  @Test
  void aRangeRemovesTheEntriesInIt() {
    assertKeyCondition("T_gt", "key > 3", k -> k > 3);
    assertKeyCondition("T_ge", "key >= 3", k -> k >= 3);
    assertKeyCondition("T_lt", "key < 3", k -> k < 3);
    assertKeyCondition("T_le", "key <= 3", k -> k <= 3);
    assertKeyCondition("T_between", "key BETWEEN 2 AND 5", k -> k >= 2 && k <= 5);
  }

  @Test
  void aRidNarrowsTheKeyToOneEntry() {
    final String target = load("T_rid");
    // the two records of key 1 are the records 1 and 2
    assertThat(delete("DELETE FROM " + target + " WHERE key = 1 AND rid = " + rids.get(1))).as("entries removed").isEqualTo(1);
    assertThat(indexedRows("T_rid", 1)).as("the entry of the other record is left").isEqualTo(1);
    try (final ResultSet rows = database.query("sql", "SELECT FROM T_rid WHERE k = 1")) {
      assertThat(rows.next().getIdentity().orElseThrow()).isEqualTo(rids.get(2));
      assertThat(rows.hasNext()).isFalse();
    }
    assertThat(records("T_rid")).isEqualTo(KEYS.length);
    assertThat(indexedRows("T_rid", 3)).as("no other key is touched").isEqualTo(3);
  }

  @Test
  void aRidOfAnotherKeyRemovesNothing() {
    final String target = load("T_rid_other");
    // the record 0 is under key 0, not 1
    assertThat(delete("DELETE FROM " + target + " WHERE key = 1 AND rid = " + rids.get(0))).as("entries removed").isZero();
    assertThat(indexedRows("T_rid_other", 1)).isEqualTo(2);
    assertThat(indexedRows("T_rid_other", 0)).isEqualTo(1);
  }

  @Test
  void theRidMayBeWrittenFirst() {
    final String target = load("T_rid_first");
    assertThat(delete("DELETE FROM " + target + " WHERE rid = " + rids.get(4) + " AND key = 3")).as("entries removed").isEqualTo(1);
    assertThat(indexedRows("T_rid_first", 3)).isEqualTo(2);
  }

  @Test
  void whatTheStatementCannotDoIsRefused() {
    final String target = load("T_refused");
    assertThatThrownBy(() -> delete("DELETE FROM " + target + " WHERE key = 1 LIMIT 1")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("LIMIT");
    assertThatThrownBy(() -> delete("DELETE FROM " + target + " WHERE key = 1 UNSAFE")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("UNSAFE");
    assertThatThrownBy(() -> delete("DELETE VERTEX FROM " + target + " WHERE key = 1")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("VERTEX");
    assertThatThrownBy(() -> delete("DELETE FROM " + target)).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("without a condition");
    assertThatThrownBy(() -> delete("DELETE FROM " + target + " WHERE key = 1 OR key = 2")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not supported");
    assertThatThrownBy(() -> delete("DELETE FROM " + target + " WHERE k = 1")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not supported");
    assertThatThrownBy(() -> delete("DELETE FROM " + target + " WHERE rid = " + rids.get(1))).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not supported");
    assertThatThrownBy(() -> delete("DELETE FROM " + target + " WHERE key = 1 AND key = 2 AND rid = " + rids.get(1)))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not supported");

    // each refusal happens before anything is removed
    assertThat(indexedRows("T_refused", 1)).isEqualTo(2);
    assertThat(indexedRows("T_refused", 2)).isEqualTo(1);
  }
}
