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

class RebuildIndexStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("REBUILD INDEX *");
    checkRightSyntax("REBUILD INDEX Foo");
    checkRightSyntax("rebuild index Foo");
    checkRightSyntax("REBUILD INDEX `Foo.bar`");
    checkRightSyntax("REBUILD INDEX `Foo.bar.baz`");
    checkRightSyntax("REBUILD INDEX * with batchSize = 1000");
    checkRightSyntax("REBUILD INDEX `Foo.bar.baz` with batchSize = 1000");

    checkWrongSyntax("REBUILD INDEX `Foo.bar` foo");
    checkRightSyntax("REBUILD INDEX `Foo.bar.baz` with unknown = 1000");
  }

  @Test
  void wildcardCapturesFirstSettingKey() throws Exception {
    // Regression for the firstSettingKeyIndex offset fix: the * (all) form has no index-name identifier, so the first WITH
    // setting (batchSize here) must still be parsed into the settings map - before the fix it was silently dropped. checkRightSyntax
    // only proves the grammar accepts it; this asserts the setting actually reaches the AST.
    final Statement stmt = new SQLAntlrParser(null).parse("REBUILD INDEX * WITH batchSize = 1000");
    assertThat(stmt).isInstanceOf(RebuildIndexStatement.class);
    final RebuildIndexStatement rebuild = (RebuildIndexStatement) stmt;
    assertThat(rebuild.all).isTrue();
    assertThat(rebuild.settings.keySet()).anyMatch(k -> "batchSize".equals(k.toString()));

    // Sanity: the named form still captures its setting too (identifier(0) is the index name, the key starts at identifier(1)).
    final RebuildIndexStatement named = (RebuildIndexStatement) new SQLAntlrParser(null).parse(
        "REBUILD INDEX `Foo.bar` WITH batchSize = 1000");
    assertThat(named.all).isFalse();
    assertThat(named.settings.keySet()).anyMatch(k -> "batchSize".equals(k.toString()));
  }
}
