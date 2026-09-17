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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7794: {@code CreateTriggerStatement} only overrode the no-arg debug {@code toString()}
 * and never {@code toString(Map, StringBuilder)}, so rendering the statement produced a Java debug dump instead of
 * SQL, and nesting it inside anything that renders through the two-argument form (an {@code IF} block, a script)
 * threw {@code UnsupportedOperationException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7794CreateTriggerToStringTest extends AbstractParserTest {

  @Test
  void reRendersAsSqlNotAsJavaDebugDump() {
    final Statement result = (Statement) checkRightSyntax(
        "CREATE TRIGGER t1 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
    final String rendered = result.toString();

    assertThat(rendered).doesNotContain("CreateTriggerStatement{");
    assertThat(rendered).isEqualTo("CREATE TRIGGER t1 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
  }

  @Test
  void reRendersIfNotExistsAndPreservesActionCodeQuoting() {
    final Statement result = (Statement) checkRightSyntax(
        "CREATE TRIGGER IF NOT EXISTS t2 AFTER UPDATE ON TYPE Purchase EXECUTE JAVASCRIPT 'record.total > 0'");
    assertThat(result.toString())
        .isEqualTo("CREATE TRIGGER IF NOT EXISTS t2 AFTER UPDATE ON TYPE Purchase EXECUTE JAVASCRIPT 'record.total > 0'");
  }

  /** Nesting inside an IF block renders through the two-arg toString(); this used to throw UnsupportedOperationException. */
  @Test
  void rendersInsideIfBlockWithoutThrowing() {
    checkRightSyntax("IF (1 = 1) { CREATE TRIGGER t3 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'; }");
  }

  @Test
  void copyPreservesEveryField() {
    final CreateTriggerStatement stmt = (CreateTriggerStatement) new SQLAntlrParser(null)
        .parse("CREATE TRIGGER IF NOT EXISTS t4 BEFORE DELETE ON TYPE User EXECUTE JAVA 'com.example.Foo'");
    final CreateTriggerStatement copy = stmt.copy();

    assertThat(copy.toString()).isEqualTo(stmt.toString());
    assertThat(copy.actionCode).isEqualTo(stmt.actionCode);
    assertThat(copy.ifNotExists).isTrue();
    assertThat(copy).isEqualTo(stmt);
  }

  /**
   * claude-review follow-up: {@code actionCodeQuoted} is only populated by the parser. A statement built any other
   * way (setting {@code actionCode} directly) must still quote it on render rather than emit the bare word "null".
   */
  @Test
  void quotesActionCodeWhenNotBuiltByTheParser() {
    final CreateTriggerStatement stmt = new CreateTriggerStatement();
    stmt.name = new Identifier("t5");
    stmt.timing = new Identifier("BEFORE");
    stmt.event = new Identifier("CREATE");
    stmt.typeName = new Identifier("User");
    stmt.actionType = new Identifier("SQL");
    stmt.actionCode = "SELECT 1";

    assertThat(stmt.toString()).isEqualTo("CREATE TRIGGER t5 BEFORE CREATE ON TYPE User EXECUTE SQL 'SELECT 1'");
  }
}
