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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Regression test for issue #5867: the legacy JavaCC/JJTree-generated SQL parser and lexer
 * ({@code SqlParser}, {@code SqlParserTokenManager}, {@code JJTSqlParserState}), the token/lexer support classes
 * only they and each other depended on ({@code Token}, {@code ParseException}, {@code TokenMgrError},
 * {@code TokenMgrException}, {@code CharStream}, {@code JavaCharStream}, {@code SimpleCharStream}), and finally
 * {@code SqlParserTreeConstants} itself (its two node-id markers, {@code JJTLIMIT}/{@code JJTTIMEOUT}, were passed
 * only to {@link SimpleNode}'s empty constructor, so every call site now uses the {@code -1} convention already
 * used everywhere else in this package) were all unreachable at runtime - {@link StatementCache} has always parsed
 * through {@link SQLAntlrParser} - and were removed, along with the {@code SQL_PARSER_IMPLEMENTATION} config it
 * never actually honored. This test guards against reintroducing any of that, and against {@link StatementCache}
 * silently gaining a second parser field.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LegacyJavaCcParserRemovalTest {

  @Test
  void legacyJavaCcGeneratedClassesAreGone() {
    for (final String className : new String[] {
        "com.arcadedb.query.sql.parser.SqlParser",
        "com.arcadedb.query.sql.parser.SqlParserTokenManager",
        "com.arcadedb.query.sql.parser.JJTSqlParserState",
        "com.arcadedb.query.sql.parser.SqlParserTreeConstants",
        "com.arcadedb.query.sql.parser.Token",
        "com.arcadedb.query.sql.parser.ParseException",
        "com.arcadedb.query.sql.parser.TokenMgrError",
        "com.arcadedb.query.sql.parser.TokenMgrException",
        "com.arcadedb.query.sql.parser.CharStream",
        "com.arcadedb.query.sql.parser.JavaCharStream",
        "com.arcadedb.query.sql.parser.SimpleCharStream" })
      assertThatExceptionOfType(ClassNotFoundException.class)
          .as("legacy JavaCC class %s must not be reintroduced", className)
          .isThrownBy(() -> Class.forName(className));
  }

  @Test
  void simpleNodeHasNoDeadTokenAccessors() {
    for (final String methodName : new String[] { "jjtGetFirstToken", "jjtSetFirstToken", "jjtGetLastToken", "jjtSetLastToken" })
      assertThat(Arrays.stream(SimpleNode.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName))
          .as("%s was a no-op JJTree leftover (always returned null / never stored anything) with no callers", methodName)
          .doesNotContain(methodName);
  }

  @Test
  void noOpParserSelectionConfigIsGone() {
    assertThat(GlobalConfiguration.findByKey("arcadedb.sql.parserImplementation"))
        .as("SQL_PARSER_IMPLEMENTATION never actually switched parsers - StatementCache always used ANTLR - "
            + "so it should not come back")
        .isNull();
  }

  @Test
  void statementCacheHasExactlyOneAntlrParserFieldAndNoOtherParserImplementation() throws Exception {
    final Field[] fields = StatementCache.class.getDeclaredFields();

    final long parserFieldCount = Arrays.stream(fields)
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .filter(f -> f.getType().getSimpleName().toLowerCase().contains("parser"))
        .count();
    assertThat(parserFieldCount).as("StatementCache should hold exactly one parser field").isEqualTo(1);

    final Field antlrParserField = StatementCache.class.getDeclaredField("antlrParser");
    assertThat(antlrParserField.getType()).isEqualTo(SQLAntlrParser.class);
  }

  @Test
  void statementCacheStillParsesThroughAntlr() {
    // a null Database is safe here only because SQLAntlrParser never dereferences it for a plain
    // "select ... limit ... timeout" - it is used to resolve schema-dependent constructs (e.g. type names),
    // none of which this statement touches.
    final StatementCache cache = new StatementCache(null, 2);
    final Statement statement = cache.get("select from foo limit 10 timeout 5000");

    assertThat(statement).isInstanceOf(SelectStatement.class);
    final SelectStatement select = (SelectStatement) statement;
    assertThat(select.getLimit().getValue(null)).isEqualTo(10);
    assertThat(select.getTimeout().val.longValue()).isEqualTo(5000L);
  }

  @Test
  void survivingConstantHolderStillExposesItsLiveConsumerNeeds() {
    final boolean hasSelectKeyword = Arrays.stream(SqlParserConstants.tokenImage)
        .anyMatch(image -> image.equals("\"select\""));
    assertThat(hasSelectKeyword).as("tokenImage must still expose the SELECT keyword used by FunctionReferenceGenerator")
        .isTrue();
  }
}
