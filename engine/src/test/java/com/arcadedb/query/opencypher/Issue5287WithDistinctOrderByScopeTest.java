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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5287: {@code WITH DISTINCT} must narrow the scope available to its
 * {@code ORDER BY} to the projected columns, exactly as {@code RETURN DISTINCT} already does.
 * <p>
 * {@code WITH DISTINCT n.active AS active ORDER BY n.name} used to be accepted. DISTINCT collapses
 * the two {@code active: true} rows into one, and that survivor keeps the {@code n} of whichever
 * duplicate arrived first, so the sort key came from an arbitrary group member and the result
 * depended on storage order. The equivalent RETURN DISTINCT form correctly raised an error; the two
 * clauses now agree, and both match Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5287WithDistinctOrderByScopeTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5287");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.command("cypher", "CREATE (:P {active: true, age: 30, name: 'c'})");
      database.command("cypher", "CREATE (:P {active: false, age: 20, name: 'b'})");
      database.command("cypher", "CREATE (:P {active: true, age: 10, name: 'a'})");
    });
  }

  @AfterEach
  public void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private List<Object> column(final String cypher, final String columnName) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("cypher", cypher)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty(columnName));
      }
    }
    return values;
  }

  /**
   * The reported case: {@code n.name} survives DISTINCT only as an arbitrary group representative,
   * so ordering by it has no deterministic result and must be rejected.
   */
  @Test
  public void withDistinctOrderByNonProjectedRejected() {
    assertThatThrownBy(
        () -> database.query("cypher", "MATCH (n:P) WITH DISTINCT n.active AS active ORDER BY n.name RETURN active").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("not defined");
  }

  /**
   * WITH DISTINCT must now agree with the RETURN DISTINCT form, which already rejected this.
   */
  @Test
  public void withDistinctAndReturnDistinctAgree() {
    assertThatThrownBy(() -> database.query("cypher", "MATCH (n:P) WITH DISTINCT n.name AS name ORDER BY n.age RETURN name").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("not defined");
    assertThatThrownBy(() -> database.query("cypher", "MATCH (n:P) RETURN DISTINCT n.name AS name ORDER BY n.age").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("not defined");
  }

  /**
   * Ordering by a projected expression stays well defined and must keep working (issue #5283).
   */
  @Test
  public void withDistinctOrderByProjectedExpressionStillWorks() {
    assertThat(column("MATCH (n:P) WITH DISTINCT n.age AS age ORDER BY n.age RETURN age", "age")) //
        .containsExactly(10, 20, 30);
  }

  /**
   * Ordering by the alias stays well defined and must keep working.
   */
  @Test
  public void withDistinctOrderByAliasStillWorks() {
    assertThat(column("MATCH (n:P) WITH DISTINCT n.age AS age ORDER BY age DESC RETURN age", "age")) //
        .containsExactly(30, 20, 10);
  }

  /**
   * An un-aliased projection is named after its own expression text, so ordering by that text
   * resolves against the projected column and the narrowing must not mistake it for a pre-DISTINCT
   * reference. (The downstream clause cannot name {@code n} here, since an un-aliased WITH does not
   * carry it forward - hence the constant projection.)
   */
  @Test
  public void withDistinctOrderByUnaliasedProjectionNotRejected() {
    // one row per distinct age; Cypher integer literals are 64-bit, hence Long
    assertThat(column("MATCH (n:P) WITH DISTINCT n.age ORDER BY n.age RETURN 1 AS one", "one")) //
        .containsExactly(1L, 1L, 1L);
  }

  /**
   * A non-DISTINCT WITH keeps the full incoming scope, so ordering by a non-projected expression
   * remains legal there: the narrowing must not leak onto plain projections.
   */
  @Test
  public void nonDistinctWithKeepsFullScope() {
    assertThat(column("MATCH (n:P) WITH n.active AS active ORDER BY n.name RETURN active", "active")) //
        .containsExactly(true, false, true);
  }
}
