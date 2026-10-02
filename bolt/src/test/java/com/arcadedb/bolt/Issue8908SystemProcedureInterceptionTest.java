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
        "CALL db.labels() YIELD label OPTIONAL MATCH (n) RETURN n" }) {
      final String normalized = BoltSystemProcedures.normalize(query);
      assertThat(BoltSystemProcedures.isSchemaProcedureQuery(normalized)).as(query).isFalse();
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
  void literalsParametersAndPropertyNamesInTheTailDoNotTripTheClauseCheck() {
    for (final String query : new String[] { "CALL dbms.listDatabases() YIELD name WHERE name = $use",
        "CALL dbms.listDatabases() YIELD name WHERE name = 'set'", "CALL dbms.listDatabases() YIELD name WHERE name = \"a match b\"",
        "CALL dbms.listDatabases() YIELD name WHERE x.create = 1", "CALL db.ping() YIELD success WHERE success" })
      assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(query),
          query.contains("ping") ? "db.ping" : "dbms.listdatabases")).as(query).isTrue();
  }

  @Test
  void boltOnlyProceduresKeepTheirTailsButNeedTheAnchor() {
    assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(
        "CALL dbms.components() YIELD name, versions, edition UNWIND versions AS version RETURN name, version, edition"),
        "dbms.components")).isTrue();
    assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(
        "CALL dbms.listDatabases() YIELD name WHERE name = 'x'"), "dbms.listdatabases")).isTrue();
    assertThat(BoltSystemProcedures.isSystemCall("call db.pingall()", "db.ping")).isFalse();
    assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(
        "MATCH (n) RETURN 'call dbms.components()'"), "dbms.components")).isFalse();
  }

  @Test
  void aBoltOnlyCallFollowedByAWriteIsLeftToTheEngine() {
    for (final String[] c : new String[][] { { "CALL db.ping() CREATE (:X)", "db.ping" },
        { "CALL db.ping() CREATE(:X)", "db.ping" }, { "CALL dbms.info() YIELD name MATCH (n) DELETE n", "dbms.info" },
        { "CALL dbms.info() YIELD name MERGE(n:X)", "dbms.info" }, { "CALL dbms.components() YIELD name SET x.y = 1", "dbms.components" },
        { "CALL db.ping() FOREACH(a IN [1] | CREATE (:Y))", "db.ping" }, { "CALL db.ping() MATCH(n) RETURN n", "db.ping" },
        { "CALL db.ping();CREATE (:X)", "db.ping" }, { "CALL db.ping()CREATE(:X)", "db.ping" },
        { "CALL db.ping() YIELD success RETURN success UNION RETURN 1 AS success", "db.ping" } })
      assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(c[0]), c[1])).as(c[0]).isFalse();
  }

  @Test
  void leadingCommentsDoNotHideTheStatement() {
    assertThat(BoltSystemProcedures.normalize("// probe\nCALL db.labels()")).isEqualTo("call db.labels()");
    assertThat(BoltSystemProcedures.normalize("/* a */ /* b */\n CALL db.ping()")).isEqualTo("call db.ping()");
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize("// x\nCALL db.labels()"))).isTrue();
  }

  @Test
  void anUnterminatedLeadingCommentLeavesNothingToServe() {
    assertThat(BoltSystemProcedures.normalize("/* never closed CALL db.ping()")).isEmpty();
  }

  @Test
  void anEscapedQuoteCannotHideAClauseInTheTail() {
    final String query = "CALL db.ping() YIELD success WHERE success = 'a\\' ' CREATE (:X {a: 'z'})";
    assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(query), "db.ping")).isFalse();
    assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(
        "CALL db.ping() YIELD success WHERE success = 'it\\'s create'"), "db.ping")).isTrue();
  }

  @Test
  void unionAllCombinedFormGoesToTheEngine() {
    assertThat(BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(
        "CALL db.labels() YIELD label RETURN label UNION ALL CALL db.relationshipTypes() YIELD relationshipType "
            + "RETURN relationshipType UNION ALL CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"))).isFalse();
  }
}
