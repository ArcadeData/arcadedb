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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12339: a conjunction of relationship types was true in a pattern ({@code [r:R&R]}) and false in a WHERE
 * ({@code r:R&R}), because the WHERE answered "an edge has one type" without testing the operands. A conjunction holds when
 * the edge type is (polymorphically, #9124) an instance of every operand. {@code [r:R&$('R')]} was refused with a message that
 * named operators the expression does not contain.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRelationshipTypeConjunctionIssue12339Test extends TestHelper {

  @BeforeEach
  void setUpGraph() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE A").close();
      database.command("sql", "CREATE EDGE TYPE B").close();
      // C extends both A and B, so a C relationship is an instance of A and of B
      database.command("sql", "CREATE EDGE TYPE C EXTENDS A, B").close();
      database.command("opencypher", "CREATE (a)-[:R]->(b), (c)-[:A]->(d), (e)-[:C]->(f)").close();
    });
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void conjunctionInWhereAgreesWithThePattern() {
    assertThat(count("MATCH ()-[r:R&R]->() RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r]->() WHERE r:R&R RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r]->() WHERE r:R:R RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r]->() WHERE r:R&A RETURN count(r) AS c")).isZero();
    assertThat(count("MATCH ()-[r:R&A]->() RETURN count(r) AS c")).isZero();
  }

  @Test
  void conjunctionIsPolymorphic() {
    assertThat(count("MATCH ()-[r]->() WHERE r:A&B RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:A&B]->() RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r]->() WHERE r:A RETURN count(r) AS c")).isEqualTo(2);
  }

  @Test
  void conjunctionWithADynamicType() {
    assertThat(count("MATCH ()-[r]->() WHERE r:R&$('R') RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r]->() WHERE r:A&$('B') RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r]->() WHERE r:$(['A', 'B']) RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:R&$('R')]->() RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:A&$('B')]->() RETURN count(r) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:R&$('A')]->() RETURN count(r) AS c")).isZero();
  }

  /** A dynamic type under '!', '%' or a mix of '&' and '|' is evaluated per relationship instead of refused. */
  @Test
  void aDynamicTypeCombinesWithEveryOperator() {
    assertThat(count("MATCH ()-[r:!$('R')]->() RETURN count(r) AS c")).isEqualTo(2);
    assertThat(count("MATCH ()-[r]->() WHERE r:!$('R') RETURN count(r) AS c")).isEqualTo(2);
    assertThat(count("MATCH ()-[r:(R|$('A'))&!$('B')]->() RETURN count(r) AS c")).isEqualTo(2);
    assertThat(count("MATCH ()-[r:%&$('A')]->() RETURN count(r) AS c")).isEqualTo(2);
    assertThat(count("MATCH ()-[r:R|$('A')&$('B')]->() RETURN count(r) AS c")).isEqualTo(2);
  }

  @Test
  void aDynamicLabelCombinesWithEveryOperatorOnANode() {
    database.transaction(() -> database.command("opencypher", "CREATE (:X), (:Y), (:X:Y)").close());
    assertThat(count("MATCH (n:!$('X')) WHERE n:X OR n:Y RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:$('X')&!$('Y')) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:($('X')|$('Y'))&!X) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n) WHERE n:$('X')&!$(['X', 'Y']) RETURN count(n) AS c")).isEqualTo(1);
  }

  @Test
  void aDynamicLabelInAWriteClauseUnderAnOperatorIsRefused() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (n:!$('X'))").close()))
        .isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (n:$any(['X']))").close()))
        .isInstanceOf(CommandParsingException.class);
  }
}
