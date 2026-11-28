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
 * Regression for issue #13583: a comment between {@code CALL} and the procedure name
 * ({@code CALL /*+ OGM READ_ONLY *&#47; dbms.components()}, the hint Neo4j-OGM puts on every read-only statement) hid the
 * call from the Bolt interception, so a {@code dbms.*} procedure, which exists nowhere else, was answered by the engine
 * with ProcedureNotFound. The Cypher lexer hides such a comment, and so does the anchoring now.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13583CommentAfterCallTest {

  @Test
  void aBlockCommentBetweenCallAndTheNameIsSkipped() {
    assertThat(BoltSystemProcedures.normalize("CALL /*+ OGM READ_ONLY */ dbms.components() yield name"))
        .isEqualTo("call dbms.components() yield name");
    assertThat(systemCall("CALL /*+ OGM READ_ONLY */ dbms.components() yield name", "dbms.components")).isTrue();
    assertThat(systemCall("CALL /* x */ db.ping()", "db.ping")).isTrue();
    assertThat(systemCall("call\n/* x */\n  dbms.listDatabases() YIELD name WHERE name = 'x'", "dbms.listdatabases")).isTrue();
  }

  @Test
  void aCommentGluedToCallOrToTheNameIsSkipped() {
    assertThat(BoltSystemProcedures.normalize("CALL/*x*/dbms.components()")).isEqualTo("call dbms.components()");
    assertThat(systemCall("CALL/*x*/dbms.components()", "dbms.components")).isTrue();
    assertThat(systemCall("CALL /*x*/dbms.info()", "dbms.info")).isTrue();
    assertThat(systemCall("CALL//x\ndb.ping()", "db.ping")).isTrue();
  }

  @Test
  void aLineCommentBetweenCallAndTheNameIsSkipped() {
    assertThat(BoltSystemProcedures.normalize("CALL // line comment\n dbms.components() YIELD name RETURN name"))
        .isEqualTo("call dbms.components() yield name return name");
    assertThat(systemCall("CALL // line comment\n dbms.components() YIELD name RETURN name", "dbms.components")).isTrue();
    assertThat(systemCall("CALL // line comment\r\n dbms.components()", "dbms.components")).isTrue();
  }

  @Test
  void stackedCommentsBetweenCallAndTheNameAreSkipped() {
    final String query = "/* lead */ CALL /* a */ // b\n /* c */\n\t// d\n dbms.components() YIELD name";
    assertThat(BoltSystemProcedures.normalize(query)).isEqualTo("call dbms.components() yield name");
    assertThat(systemCall(query, "dbms.components")).isTrue();
  }

  @Test
  void theSchemaProceduresAnchorTheSameWay() {
    assertThat(schemaQuery("CALL /*+ OGM READ_ONLY */ db.labels()")).isTrue();
    assertThat(schemaQuery("CALL // x\n db.propertyKeys() YIELD propertyKey")).isTrue();
    assertThat(BoltSystemProcedures.serveSchemaProcedure(null,
        BoltSystemProcedures.normalize("CALL /* x */ db.relationshipTypes()"))).isNotNull();
    // What the issue lists as answered by the engine stays the engine's
    assertThat(schemaQuery("CALL /*+ OGM READ_ONLY */ db.labels() YIELD label RETURN count(label) >= 0 AS ok")).isFalse();
    // Arguments cannot be evaluated here, commented or not
    assertThat(BoltSystemProcedures.serveSchemaProcedure(null, BoltSystemProcedures.normalize("CALL /* x */ db.labels('a')"))).isNull();
  }

  @Test
  void aWriteAfterACommentedCallIsStillLeftToTheEngine() {
    for (final String[] c : new String[][] { { "CALL /*+ OGM READ_ONLY */ dbms.components() YIELD name CREATE (:X {n: name})",
        "dbms.components" }, { "CALL /* x */ db.ping() CREATE (:X)", "db.ping" },
        { "CALL // x\n db.ping() MATCH (n) DELETE n", "db.ping" }, { "CALL /* x */ dbms.info() YIELD name MERGE (n:X)", "dbms.info" },
        { "CALL /* x */ db.ping() // y\nCREATE (:X)", "db.ping" }, { "CALL /* x */ db.ping();CREATE (:X)", "db.ping" } })
      assertThat(systemCall(c[0], c[1])).as(c[0]).isFalse();
    assertThat(schemaQuery("CALL /* x */ db.labels() YIELD label CREATE (:X {n: label})")).isFalse();
  }

  @Test
  void aCommentThatHidesTheNameLeavesNothingToServe() {
    // The name is inside the comment: what is called is whatever follows it
    assertThat(systemCall("CALL /* dbms.components() */ foo.bar()", "dbms.components")).isFalse();
    assertThat(systemCall("CALL // dbms.components()\n foo.bar()", "dbms.components")).isFalse();
    assertThat(systemCall("CALL // db.ping()", "db.ping")).isFalse();
    assertThat(systemCall("CALL /* x */ CREATE (:X)", "db.ping")).isFalse();
    assertThat(systemCall("CALL /* x */ { CREATE (:X) } CALL db.ping()", "db.ping")).isFalse();
  }

  @Test
  void anUnterminatedCommentAfterCallLeavesNothingToServe() {
    assertThat(systemCall("CALL /* never closed dbms.components()", "dbms.components")).isFalse();
    assertThat(systemCall("CALL /* never closed\n dbms.components()", "dbms.components")).isFalse();
    assertThat(schemaQuery("CALL /* never closed db.labels()")).isFalse();
  }

  @Test
  void aStatementThatOnlyStartsWithTheLettersOfCallIsUntouched() {
    assertThat(BoltSystemProcedures.normalize("CALLING /* x */ dbms.components()")).isEqualTo("calling /* x */ dbms.components()");
    assertThat(systemCall("CALLING /* x */ dbms.components()", "dbms.components")).isFalse();
    assertThat(BoltSystemProcedures.normalize("CALL")).isEqualTo("call");
    assertThat(BoltSystemProcedures.normalize("CALL db.ping() /* x */")).isEqualTo("call db.ping() /* x */");
  }

  /**
   * The Cypher lexer ends a line comment at a carriage return as well as at a line feed, and closes a block comment at the
   * first star-slash AFTER its opener. Skipping more, or less, than the lexer hides would anchor on a statement other than the
   * one the engine runs, and answer a write with a canned row.
   */
  @Test
  void aCommentEndsWhereTheCypherLexerEndsIt() {
    // The engine reads: CREATE (:X) CALL db.ping()
    assertThat(systemCall("// x\rCREATE (:X)\nCALL db.ping()", "db.ping")).isFalse();
    // The engine reads: CALL db.labels() YIELD label CREATE (:X {a: db.ping })
    assertThat(systemCall("CALL // x\rdb.labels() YIELD label CREATE (:X {a:\n db.ping })", "db.ping")).isFalse();
    assertThat(systemCall("CALL // x\r dbms.components()", "dbms.components")).isTrue();
    // The engine reads: RETURN 1, the whole of /*/ ... */ being one comment
    assertThat(systemCall("/*/ CALL db.ping() /* */ RETURN 1", "db.ping")).isFalse();
    assertThat(systemCall("CALL /*/ db.ping() /* */ foo.bar()", "db.ping")).isFalse();
    assertThat(systemCall("CALL /*/ x */ db.ping()", "db.ping")).isTrue();
  }

  private static boolean systemCall(final String query, final String procedureName) {
    return BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize(query), procedureName);
  }

  private static boolean schemaQuery(final String query) {
    return BoltSystemProcedures.isSchemaProcedureQuery(BoltSystemProcedures.normalize(query));
  }
}
