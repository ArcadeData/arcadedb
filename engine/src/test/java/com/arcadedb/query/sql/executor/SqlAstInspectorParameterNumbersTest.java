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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract of {@link SqlAstInspector#parameterNumbersSuffix} (issue #9247): empty when the parameters are numbered 0, 1, 2...
 * as in a statement parsed alone, the numbers otherwise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlAstInspectorParameterNumbersTest extends TestHelper {

  private List<Statement> script(final String script) {
    return new SQLAntlrParser(database).parseScript(script);
  }

  @Test
  void noParameters() {
    assertThat(SqlAstInspector.parameterNumbersSuffix(script("SELECT FROM V WHERE a = 1;").get(0))).isEmpty();
  }

  @Test
  void sequentialNumbering() {
    assertThat(SqlAstInspector.parameterNumbersSuffix(script("SELECT FROM V WHERE a = ? AND b = ?;").get(0))).isEmpty();
  }

  @Test
  void offsetNumberingInAScript() {
    final List<Statement> statements = script("SELECT FROM V WHERE a = ?; SELECT FROM V WHERE a = ?;");
    assertThat(SqlAstInspector.parameterNumbersSuffix(statements.get(0))).isEmpty();
    assertThat(SqlAstInspector.parameterNumbersSuffix(statements.get(1))).isEqualTo(" /*params:1*/");
  }

  @Test
  void mixedNamedAndPositional() {
    final List<Statement> statements = script("SELECT FROM V WHERE a = ? AND b = :x; SELECT FROM V WHERE a = :y AND b = ?;");
    assertThat(SqlAstInspector.parameterNumbersSuffix(statements.get(0))).isEmpty();
    assertThat(SqlAstInspector.parameterNumbersSuffix(statements.get(1))).isEqualTo(" /*params:2,3*/");
  }

  @Test
  void scriptStatementKeyCarriesTheSuffix() {
    final List<Statement> statements = script("SELECT FROM V WHERE a = ?; SELECT FROM V WHERE a = ?;");
    for (final Statement statement : statements)
      statement.setOriginalStatement(statement);
    assertThat(statements.get(0).getOriginalStatement()).doesNotContain("/*params:");
    assertThat(statements.get(1).getOriginalStatement()).endsWith(" /*params:1*/");
  }
}
