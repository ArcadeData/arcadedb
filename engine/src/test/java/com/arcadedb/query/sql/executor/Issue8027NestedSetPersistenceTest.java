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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8027.
 * <p>
 * {@code UPDATE ... SET <prop>.<key> = v} and {@code UPDATE ... SET <prop>[<index>] = v} reach into the container
 * the record already holds and mutate it IN PLACE, never going through {@code MutableDocument.set()} - the only
 * thing that sets the record's dirty flag. {@code SaveElementStep} then skips the save of a record it believes
 * clean (a skip that exists to avoid a needless MVCC version bump), so the write was lost. Three things made it
 * silent: the statement reported {@code count: 1}, {@code RETURN AFTER} reported the CHANGED value because it
 * reads back the mutated in-memory row, and both whole-property SET and nested SET on an embedded document
 * worked.
 * <p>
 * The REMOVE side of exactly this was fixed for issue #4730 and has carried a {@code markOwnerDirty} call since;
 * the SET side never got one. Every assertion below re-reads the value in a LATER transaction, which is the only
 * place the loss was visible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8027NestedSetPersistenceTest extends TestHelper {

  private static final String TYPE = "Issue8027Doc";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
      database.command("sql", "CREATE DOCUMENT TYPE Issue8027Emb");
    });
  }

  @Test
  void aNestedMapKeySetWithDotNotationSurvivesTheCommit() {
    insert();
    update("UPDATE " + TYPE + " SET m.a = 9");
    assertThat(map()).containsEntry("a", 9).containsEntry("b", 2);
  }

  @Test
  void aNestedMapKeySetWithBracketNotationSurvivesTheCommit() {
    insert();
    update("UPDATE " + TYPE + " SET m['a'] = 8");
    assertThat(map()).containsEntry("a", 8).containsEntry("b", 2);
  }

  /** A key the map did not carry: the in-place put ADDS it, and that addition was lost the same way. */
  @Test
  void aNewNestedMapKeySurvivesTheCommit() {
    insert();
    update("UPDATE " + TYPE + " SET m.c = 3");
    assertThat(map()).containsEntry("a", 1).containsEntry("b", 2).containsEntry("c", 3);
  }

  @Test
  void aListElementSetByIndexSurvivesTheCommit() {
    insert();
    update("UPDATE " + TYPE + " SET l[0] = 'Q'");
    assertThat(list()).containsExactly("Q", "y", "z");
  }

  @Test
  void aListRangeSetSurvivesTheCommit() {
    insert();
    update("UPDATE " + TYPE + " SET l[0..2] = 'R'");
    assertThat(list()).containsExactly("R", "R", "z");
  }

  /**
   * {@code RETURN AFTER} is what an application uses to verify the write, and it reported the change even when the
   * change never reached disk. It must keep reporting it - and now agree with what a later transaction reads.
   */
  @Test
  void returnAfterAgreesWithWhatTheNextTransactionReads() {
    insert();
    final Object[] returned = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE " + TYPE + " SET m.a = 9 RETURN AFTER")) {
        returned[0] = rs.next().<Map<String, Object>>getProperty("m").get("a");
      }
    });
    assertThat(returned[0]).isEqualTo(9);
    assertThat(map()).containsEntry("a", 9);
  }

  /** The two shapes that always worked, kept as the control the issue measures the broken ones against. */
  @Test
  void theShapesThatAlreadyWorkedStillWork() {
    insert();

    update("UPDATE " + TYPE + " SET m = {'a': 7, 'b': 2}");
    assertThat(map()).containsEntry("a", 7);

    update("UPDATE " + TYPE + " SET emb.x = 7");
    assertThat(embeddedX()).isEqualTo(7);

    update("UPDATE " + TYPE + " REMOVE m.b");
    assertThat(map()).doesNotContainKey("b");
  }

  private void insert() {
    database.transaction(() -> database.command("sql", "INSERT INTO " + TYPE
        + " SET k = 1, m = {'a':1,'b':2}, l = ['x','y','z'], emb = {'@type':'Issue8027Emb','x':1,'y':2}"));
  }

  private void update(final String sql) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
  }

  private Map<String, Object> map() {
    return read("m");
  }

  private List<String> list() {
    return read("l");
  }

  private Object embeddedX() {
    final Object embedded = read("emb");
    return embedded instanceof com.arcadedb.database.Document doc ? doc.get("x") : ((Map<?, ?>) embedded).get("x");
  }

  private <T> T read(final String property) {
    final Object[] holder = new Object[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
        holder[0] = rs.next().getProperty(property);
      }
    });
    return (T) holder[0];
  }
}
