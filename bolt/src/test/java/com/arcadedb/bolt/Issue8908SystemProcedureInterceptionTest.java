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
package com.arcadedb.bolt;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8908: the Bolt executor answered any query whose text merely MENTIONED a system procedure
 * ({@code db.propertyKeys}, {@code dbms.components}, {@code db.ping}...) with that procedure's canned rows, so a
 * larger statement that called one inside a {@code CALL (*) { ... UNION ... }} subquery was never run by the engine.
 * Only a statement that IS the procedure call may be served here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8908SystemProcedureInterceptionTest {
  private static final String REPORTED = """
      CALL (*) {
        RETURN null AS x
        UNION
        CALL db.propertyKeys() YIELD propertyKey
        RETURN null AS x
      }
      WITH x
      WHERE x IS NULL
      LOAD CSV FROM 'file:///tmp/arcade-load.csv' AS row
      WITH x, row
      RETURN x
      """;

  @Test
  void theReportedQueryIsLeftToTheEngine() {
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(REPORTED))).isFalse();
    assertThat(BoltSystemProcedures.isStandaloneCall(BoltSystemProcedures.normalize(REPORTED), "db.propertykeys")).isFalse();
    assertThat(BoltSystemProcedures.isStandaloneCall(BoltSystemProcedures.normalize(
        "MATCH (n) RETURN n, 'dbms.components' AS s"), "dbms.components")).isFalse();
  }

  @Test
  void aStatementThatIsTheCallIsStillServed() {
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize("CALL db.propertyKeys()"))).isTrue();
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(
        BoltSystemProcedures.normalize("  call   DB.labels() YIELD label"))).isTrue();
    assertThat(BoltSystemProcedures.isStandaloneCall(BoltSystemProcedures.normalize("CALL dbms.components() YIELD name"),
        "dbms.components")).isTrue();
    assertThat(BoltSystemProcedures.isStandaloneCall(BoltSystemProcedures.normalize("CALL db.ping()"), "db.ping")).isTrue();
  }

  @Test
  void theCombinedDesktopQueryIsStillServed() {
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(
        "CALL db.labels() YIELD label RETURN collect(label) AS result "
            + "UNION CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) AS result "
            + "UNION CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) AS result"))).isTrue();
  }

  @Test
  void theNameMustEndAtATokenBoundary() {
    assertThat(BoltSystemProcedures.isStandaloneCall("call db.pingall()", "db.ping")).isFalse();
    assertThat(BoltSystemProcedures.isStandaloneCall("call dbms.infofoo()", "dbms.info")).isFalse();
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery("call db.labelsextended()")).isFalse();
    assertThat(BoltSystemProcedures.isStandaloneCall("call db.ping", "db.ping")).isTrue();
  }

  @Test
  void aCallThatContinuesIntoOtherClausesIsLeftToTheEngine() {
    final String query = BoltSystemProcedures.normalize(
        "CALL db.labels() YIELD label MATCH (n) WHERE n.k = 'db.propertykeys' RETURN label");
    assertThat(BoltSystemProcedures.serveSchemaProcedure(null, query)).isNull();
    assertThat(BoltSystemProcedures.serveSchemaProcedure(null,
        BoltSystemProcedures.normalize("CALL db.labels() YIELD label WITH label RETURN label"))).isNull();
    assertThat(BoltSystemProcedures.serveSchemaProcedure(null,
        BoltSystemProcedures.normalize("CALL db.labels() YIELD label"))).isNotNull();
  }

  @Test
  void aWriteOrAnyOtherClauseAfterTheCallIsLeftToTheEngine() {
    for (final String query : new String[] { "CALL db.labels() YIELD label CREATE (:X {n: label})",
        "CALL db.labels() YIELD label MERGE (:X {n: label})", "CALL db.labels() YIELD label SET x.y = label",
        "CALL db.labels() YIELD label DELETE x", "CALL db.labels() YIELD label FOREACH (a IN [1] | CREATE (:Y))",
        "CALL db.labels() YIELD label OPTIONAL MATCH (n) RETURN n",
        "CALL dbms.info() YIELD id CREATE (:X)" }) {
      final String normalized = BoltSystemProcedures.normalize(query);
      assertThat(BoltSystemProcedures.isSchemaProcedureQuery(normalized)).as(query).isFalse();
      assertThat(BoltSystemProcedures.isStandaloneCall(normalized, "dbms.info")).as(query).isFalse();
    }
  }

  @Test
  void theCombinedFormWithExtraClausesOrMissingProceduresIsLeftToTheEngine() {
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(
        "CALL db.labels() YIELD label MATCH (n) WHERE n.k IN ['db.relationshiptypes','db.propertykeys'] RETURN n"))).isFalse();
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(
        "CALL db.labels() YIELD label RETURN label UNION CALL db.labels() YIELD label RETURN label "
            + "UNION CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"))).isFalse();
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(
        "CALL db.labels() YIELD label RETURN label UNION CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"))).isFalse();
  }

  @Test
  void aTrailingSemicolonIsStillServed() {
    assertThat(BoltSystemProcedures.isStandaloneCall("call db.ping;", "db.ping")).isTrue();
    assertThat(BoltSystemProcedures.isStandaloneCall("call db.ping() ;", "db.ping")).isTrue();
  }

  @Test
  void showCommandsAreAnchoredToo() {
    assertThat(BoltSystemProcedures.normalize("MATCH (n) RETURN 'show current user'").startsWith("show current user")).isFalse();
  }
}
