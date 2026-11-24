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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13282: the importing {@code WITH} of an implicit-scope {@code CALL { ... }} imports an outer variable only as a plain
 * reference. {@code WITH i AS q} and {@code WITH i + 1 AS q} imported nothing and the body read {@code q} as NULL, which a
 * writing body then wrote. Neo4j 5.26 refuses every refused statement below ("Importing WITH should consist only of simple
 * references to outer variables") and returns the rows listed for every control.
 */
class CypherCallImportingWithAliasIssue13282Test extends TestHelper {
  @Test
  void anAliasedOrComputedImportIsRefused() {
    for (final String query : List.of(
        "UNWIND [1, 2] AS i CALL { WITH i AS q RETURN q AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL { WITH i + 1 AS q RETURN q AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL { WITH i AS q WITH q RETURN q AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL { WITH i AS q RETURN q AS j UNION WITH i RETURN i AS j } RETURN i, j ORDER BY i, j"))
      assertThatThrownBy(() -> drain(query), query).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("imports an outer variable only as a plain reference");
  }

  @Test
  void aWritingBodyWithAnAliasedImportIsRefusedAndWritesNothing() {
    database.getSchema().createVertexType("C");
    assertThatThrownBy(() -> drain("UNWIND [1, 2] AS i CALL { WITH i AS q CREATE (:C {q: q}) } IN TRANSACTIONS OF 1 ROW"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("plain reference");
    assertThat(drain("MATCH (c:C) RETURN c.q AS q")).isEmpty();
  }

  @Test
  void aPlainReferenceAStarAndAScopeListStillImport() {
    for (final String query : List.of(
        "UNWIND [1, 2] AS i CALL { WITH i RETURN i AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL { WITH i AS i RETURN i AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL { WITH * RETURN i AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL (i) { WITH i AS q RETURN q AS j } RETURN i, j ORDER BY i",
        "UNWIND [1, 2] AS i CALL { WITH i WITH i AS q RETURN q AS j } RETURN i, j ORDER BY i"))
      assertThat(drain(query)).as(query).containsExactly("1,1", "2,2");
  }

  @Test
  void aLeadingWithThatReadsNoOuterVariableIsAnOrdinaryProjection() {
    assertThat(drain("UNWIND [1, 2] AS i CALL { WITH 1 AS x RETURN x } RETURN i, x ORDER BY i")).containsExactly("1,1", "2,1");
  }

  private List<String> drain(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        final List<String> cells = new ArrayList<>();
        for (final String name : row.getPropertyNames())
          cells.add(String.valueOf(row.<Object>getProperty(name)));
        rows.add(String.join(",", cells));
      }
    }
    return rows;
  }
}
