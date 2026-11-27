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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #13452 (grouped under #13665): a label predicate ({@code c:Chunk}) is a boolean expression,
 * so it may be the left operand of {@code IS [NOT] NULL} and of {@code IN}. Both were refused with "Unexpected input 'IS'" /
 * "'IN'" while the parenthesised form {@code (c:Chunk) IS NULL} was accepted. The expected values are the ones Neo4j 5.26
 * answers.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherLabelPredicateOperandIssue13452Test extends TestHelper {
  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Chunk {id: 1}), (:Record {id: 2})").close());
  }

  private List<Object> column(final String query, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  @Test
  void isNullOfALabelPredicateInAProjection() {
    assertThat(column("MATCH (c) RETURN c.id AS id, c:Chunk IS NULL AS r ORDER BY id", "r")).containsExactly(false, false);
  }

  @Test
  void isNotNullOfALabelPredicateInAProjection() {
    assertThat(column("MATCH (c) RETURN c.id AS id, c:Chunk IS NOT NULL AS r ORDER BY id", "r")).containsExactly(true, true);
  }

  @Test
  void isNotNullOfALabelPredicateInAWhere() {
    assertThat(column("MATCH (c) WHERE c:Chunk IS NOT NULL RETURN c.id AS id ORDER BY id", "id")).containsExactly(1, 2);
  }

  @Test
  void isNullOfALabelPredicateInAWhere() {
    assertThat(column("MATCH (c) WHERE c:Chunk IS NULL RETURN c.id AS id ORDER BY id", "id")).isEmpty();
  }

  @Test
  void inOfALabelPredicateInAProjection() {
    assertThat(column("MATCH (c) RETURN c.id AS id, c:Chunk IN [true] AS r ORDER BY id", "r")).containsExactly(true, false);
  }

  @Test
  void inOfALabelPredicateInAWhere() {
    assertThat(column("MATCH (c) WHERE c:Chunk IN [true] RETURN c.id AS id ORDER BY id", "id")).containsExactly(1);
    assertThat(column("MATCH (c) WHERE c:Chunk IN [false] RETURN c.id AS id ORDER BY id", "id")).containsExactly(2);
  }

  @Test
  void theLabelPredicateOfANullIsNull() {
    // a label test on null is null, so IS NULL is the one place where the predicate and its operand differ
    assertThat(column("OPTIONAL MATCH (c:Missing) RETURN c:Chunk IS NULL AS r", "r")).containsExactly(true);
    assertThat(column("OPTIONAL MATCH (c:Missing) RETURN c:Chunk IS NOT NULL AS r", "r")).containsExactly(false);
  }

  @Test
  void theOperandKeepsEveryFormOfTheLabelExpression() {
    assertThat(column("MATCH (c) RETURN c.id AS id, c:Chunk|Record IN [true] AS r ORDER BY id", "r")).containsExactly(true, true);
    assertThat(column("MATCH (c) RETURN c.id AS id, c:!Chunk IN [true] AS r ORDER BY id", "r")).containsExactly(false, true);
    assertThat(column("MATCH (c) RETURN c.id AS id, c IS Chunk IN [true] AS r ORDER BY id", "r")).containsExactly(true, false);
    assertThat(column("MATCH (c) RETURN c.id AS id, c IS Chunk IS NOT NULL AS r ORDER BY id", "r")).containsExactly(true, true);
  }

  @Test
  void theOperandCombinesWithBooleanOperatorsAndComparisons() {
    assertThat(column("MATCH (c) WHERE c:Chunk IS NOT NULL AND c.id > 1 RETURN c.id AS id ORDER BY id", "id")).containsExactly(2);
    assertThat(column("MATCH (c) WHERE NOT c:Chunk IN [true] RETURN c.id AS id ORDER BY id", "id")).containsExactly(2);
    assertThat(column("MATCH (c) RETURN c.id AS id, (c:Chunk IN [true]) = true AS r ORDER BY id", "r")).containsExactly(true, false);
    assertThat(column("MATCH (c) RETURN c.id AS id, CASE WHEN c:Chunk IN [true] THEN 'y' ELSE 'n' END AS r ORDER BY id", "r"))
        .containsExactly("y", "n");
  }

  @Test
  void theFormsThatAlreadyWorkedAreUnchanged() {
    assertThat(column("MATCH (c) RETURN c.id AS id, (c:Chunk) IS NULL AS r ORDER BY id", "r")).containsExactly(false, false);
    assertThat(column("MATCH (c) RETURN c.id AS id, c:Chunk = true AS r ORDER BY id", "r")).containsExactly(true, false);
    assertThat(column("MATCH (c) RETURN c.id AS id, c:Chunk AS r ORDER BY id", "r")).containsExactly(true, false);
    assertThat(column("MATCH (c) WHERE c:Chunk RETURN c.id AS id ORDER BY id", "id")).containsExactly(1);
    assertThat(column("MATCH (c) WHERE c.id IS NOT NULL RETURN c.id AS id ORDER BY id", "id")).containsExactly(1, 2);
  }

  @Test
  void aSecondPredicateOnAnythingButALabelPredicateIsStillRefused() {
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (c) RETURN c.id IS NULL IS NULL AS r").close())
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (c) RETURN c.id IN [1] IN [true] AS r").close())
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (c) WHERE c.id IS NULL IS NOT NULL RETURN c.id AS r").close())
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (c) RETURN c.id IS NULL IN [true] AS r").close())
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> database.query("opencypher", "RETURN 'a' IS NORMALIZED IS NULL AS r").close())
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> database.query("opencypher", "MATCH (c) RETURN c:Chunk IS NULL IS NULL AS r").close())
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void theNullAndNormalFormPredicatesAreUnchanged() {
    assertThat(column("MATCH (c) RETURN c.id AS id, c.missing IS NULL AS r ORDER BY id", "r")).containsExactly(true, true);
    assertThat(column("RETURN 'a' IS NORMALIZED AS r", "r")).containsExactly(true);
    assertThat(column("RETURN 'a' IS NOT NFC NORMALIZED AS r", "r")).containsExactly(false);
  }
}
