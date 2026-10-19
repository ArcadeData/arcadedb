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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10269: on a {@code NOTUNIQUE_HASH} index a record whose key changed away and back in one transaction ({@code 1 -> 4 -> 1})
 * was indexed twice under the key it came back to, because the replay put the pair again on top of the committed one and a
 * hash bucket appends a RID without asking whether it is already there. A later delete or key change removed one of the two
 * entries and left the record listed under a key it no longer held.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10269HashIndexKeyRoundTripTest extends TestHelper {

  private String create(final String type, final String kind) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type).close();
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER").close();
    database.command("sql", "CREATE INDEX ON " + type + " (k) " + kind).close();
    return type;
  }

  private RID insert(final String type, final int k) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(type).set("k", k).save().getIdentity());
    return rid[0];
  }

  private void roundTrip(final RID rid) {
    database.transaction(() -> {
      rid.asDocument().modify().set("k", 4).save();
      rid.asDocument().modify().set("k", 1).save();
    });
  }

  private Index index(final String type) {
    return database.getSchema().getType(type).getPolymorphicIndexByProperties("k");
  }

  private List<RID> indexed(final String type, final int k) {
    final List<RID> rids = new ArrayList<>();
    final IndexCursor cursor = index(type).get(new Object[] { k });
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity());
    return rids;
  }

  private long count(final String type, final int k) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type + " WHERE k = ?", k)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
  void keyChangedAwayAndBackIsIndexedOnce(final String kind) {
    final String t = create("A", kind);
    final RID x = insert(t, 1);

    roundTrip(x);

    assertThat(indexed(t, 1)).containsExactly(x);
    assertThat(indexed(t, 4)).isEmpty();
    assertThat(index(t).countEntries()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
  void deleteAfterTheRoundTripLeavesNoEntry(final String kind) {
    final String t = create("B", kind);
    final RID x = insert(t, 1);
    roundTrip(x);

    database.transaction(() -> x.asDocument().delete());

    assertThat(indexed(t, 1)).isEmpty();
    assertThat(count(t, 1)).isZero();
    assertThat(index(t).countEntries()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
  void keyChangeAfterTheRoundTripLeavesNoEntryUnderTheOldKey(final String kind) {
    final String t = create("C", kind);
    final RID x = insert(t, 1);
    roundTrip(x);

    database.transaction(() -> x.asDocument().modify().set("k", 2).save());

    assertThat(indexed(t, 1)).isEmpty();
    assertThat(indexed(t, 2)).containsExactly(x);
    assertThat(count(t, 1)).isZero();
    assertThat(count(t, 2)).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE", "UNIQUE_HASH", "UNIQUE" })
  void roundTripInsideTheTransactionThatDeletesTheRecord(final String kind) {
    final String t = create("D", kind);
    final RID x = insert(t, 1);

    database.transaction(() -> {
      x.asDocument().modify().set("k", 4).save();
      x.asDocument().modify().set("k", 1).save();
      x.asDocument().delete();
    });

    assertThat(indexed(t, 1)).isEmpty();
    assertThat(indexed(t, 4)).isEmpty();
    assertThat(index(t).countEntries()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  void aSiblingUnderTheSameKeyIsLeftAlone(final String kind) {
    final String t = create("E", kind);
    final RID x = insert(t, 1);
    final RID y = insert(t, 1);

    roundTrip(x);
    database.transaction(() -> x.asDocument().delete());

    assertThat(indexed(t, 1)).containsExactly(y);
    assertThat(count(t, 1)).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  void keyChangedAwayAndBackTwiceInOneTransactionIsIndexedOnce(final String kind) {
    final String t = create("F", kind);
    final RID x = insert(t, 1);

    database.transaction(() -> {
      for (int i = 0; i < 2; i++) {
        x.asDocument().modify().set("k", 4).save();
        x.asDocument().modify().set("k", 1).save();
      }
    });
    assertThat(indexed(t, 1)).containsExactly(x);

    database.transaction(() -> x.asDocument().delete());

    assertThat(indexed(t, 1)).isEmpty();
    assertThat(index(t).countEntries()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  void aRecordCreatedAndRoundTrippedInOneTransactionIsIndexedOnce(final String kind) {
    final String t = create("G", kind);
    final RID[] x = new RID[1];

    database.transaction(() -> {
      x[0] = database.newDocument(t).set("k", 1).save().getIdentity();
      x[0].asDocument().modify().set("k", 4).save();
      x[0].asDocument().modify().set("k", 1).save();
    });
    assertThat(indexed(t, 1)).containsExactly(x[0]);
    assertThat(indexed(t, 4)).isEmpty();

    database.transaction(() -> x[0].asDocument().delete());

    assertThat(indexed(t, 1)).isEmpty();
    assertThat(index(t).countEntries()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  void theRoundTripIsConsistentInsideTheTransactionToo(final String kind) {
    final String t = create("H", kind);
    final RID x = insert(t, 1);

    database.transaction(() -> {
      x.asDocument().modify().set("k", 4).save();
      x.asDocument().modify().set("k", 1).save();

      assertThat(indexed(t, 1)).containsExactly(x);
      assertThat(indexed(t, 4)).isEmpty();
      assertThat(count(t, 1)).isEqualTo(1);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  void theRoundTripAndTheDeleteSurviveAReopen(final String kind) {
    final String t = create("I", kind);
    final RID x = insert(t, 1);
    roundTrip(x);

    reopenDatabase();

    final RID[] reloaded = new RID[1];
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + t + " WHERE k = 1")) {
      reloaded[0] = rs.next().getIdentity().orElseThrow();
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(indexed(t, 1)).containsExactly(reloaded[0]);

    database.transaction(() -> reloaded[0].asDocument().delete());

    assertThat(indexed(t, 1)).isEmpty();
    assertThat(index(t).countEntries()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  void compositeKeyRoundTrip(final String kind) {
    database.command("sql", "CREATE DOCUMENT TYPE Comp").close();
    database.command("sql", "CREATE PROPERTY Comp.a INTEGER").close();
    database.command("sql", "CREATE PROPERTY Comp.b INTEGER").close();
    database.command("sql", "CREATE INDEX ON Comp (a, b) " + kind).close();
    final RID[] x = new RID[1];
    database.transaction(() -> x[0] = database.newDocument("Comp").set("a", 1).set("b", 7).save().getIdentity());

    database.transaction(() -> {
      x[0].asDocument().modify().set("a", 4).save();
      x[0].asDocument().modify().set("a", 1).save();
    });
    database.transaction(() -> x[0].asDocument().modify().set("a", 2).save());

    final IndexCursor cursor = database.getSchema().getType("Comp").getPolymorphicIndexByProperties("a", "b").get(new Object[] { 1, 7 });
    assertThat(cursor.hasNext()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT a FROM Comp WHERE a = 1 AND b = 7")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
