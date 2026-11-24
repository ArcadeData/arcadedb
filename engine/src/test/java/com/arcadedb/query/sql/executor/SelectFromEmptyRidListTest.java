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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A SELECT whose target is the literal empty list {@code []} has no record to read: it answers no rows (one zero row for a
 * global aggregate), as a RID list that filtering emptied does, instead of failing with a bare {@code UnsupportedOperationException}
 * from the planner.
 */
class SelectFromEmptyRidListTest extends TestHelper {

  private List<Result> query(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().toList();
    }
  }

  private List<Result> command(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
      return rs.stream().toList();
    }
  }

  @Test
  void emptyRidListIsAnEmptyTarget() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> database.newDocument("D").set("x", 1).save());

    assertThat(query("SELECT FROM []")).isEmpty();
    assertThat(query("SELECT @rid FROM []")).isEmpty();
    assertThat(query("SELECT FROM [] WHERE x = 1")).isEmpty();
    assertThat(query("SELECT FROM [] ORDER BY x LIMIT 5")).isEmpty();

    final List<Result> count = query("SELECT count(*) AS c FROM []");
    assertThat(count).hasSize(1);
    assertThat(count.getFirst().<Long>getProperty("c")).isZero();
  }

  /**
   * Control: the RID-list branch the empty list now reaches still answers the records of a non-empty list.
   */
  @Test
  void nonEmptyRidListStillAnswersItsRecords() {
    database.getSchema().createDocumentType("D");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("x", 1).save().getIdentity());

    final List<Result> one = query("SELECT FROM [" + rid[0] + "]");
    assertThat(one).hasSize(1);
    assertThat(one.getFirst().<Integer>getProperty("x")).isEqualTo(1);

    final List<Result> list = query("SELECT FROM [" + rid[0] + ", " + rid[0] + "]");
    assertThat(list).hasSize(2);
  }

  /**
   * TRAVERSE shares the FROM clause grammar, and its planner had the same {@code !isEmpty()} guard: {@code TRAVERSE ... FROM []}
   * failed with {@code UnsupportedOperationException("Target ''")}.
   */
  @Test
  void traverseFromEmptyRidListAnswersNoRows() {
    database.getSchema().createVertexType("V1");
    database.getSchema().createEdgeType("E1");
    database.transaction(() -> {
      final var a = database.newVertex("V1").save();
      final var b = database.newVertex("V1").save();
      a.newEdge("E1", b).save();
    });

    assertThat(query("TRAVERSE out() FROM []")).isEmpty();
    assertThat(query("TRAVERSE out() FROM [] MAXDEPTH 3")).isEmpty();
  }

  /**
   * UPDATE and DELETE over the empty list go through the SELECT planner: they touch nothing and leave the type as it was.
   */
  @Test
  void updateAndDeleteOverEmptyRidListTouchNothing() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> database.newDocument("D").set("x", 1).save());

    database.transaction(() -> {
      final List<Result> updated = command("UPDATE [] SET x = 2");
      assertThat(updated).hasSize(1);
      assertThat(updated.getFirst().<Long>getProperty("count")).isZero();

      final List<Result> deleted = command("DELETE FROM []");
      assertThat(deleted).hasSize(1);
      assertThat(deleted.getFirst().<Long>getProperty("count")).isZero();
    });

    final List<Result> rows = query("SELECT x FROM D");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Integer>getProperty("x")).isEqualTo(1);
  }

  /**
   * The empty list renders as {@code []}, so the statement text re-parses to the same statement (it used to render as
   * {@code SELECT FROM }, which does not parse).
   */
  @Test
  void emptyRidListRendersAsEmptyBrackets() {
    for (final String sql : new String[] { "SELECT FROM []", "TRAVERSE out() FROM []" }) {
      final Statement stmt = new SQLAntlrParser(null).parse(sql);
      final String rendered = stmt.toString();
      assertThat(rendered).as("rendering of '%s'", sql).contains("[]");
      assertThat(new SQLAntlrParser(null).parse(rendered).toString()).isEqualTo(rendered);
    }
  }
}
