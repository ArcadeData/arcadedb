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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLASTBuilder;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11796: TRUNCATE RECORD is recognised only to be refused while it is parsed, with a message naming the statements that do
 * its job, instead of parsing, planning and failing every time with a bare UnsupportedOperationException.
 */
class TruncateRecordStatementTestParserTest extends AbstractParserTest {

  @Test
  void refusedWhileParsed() {
    for (final String query : new String[] { "TRUNCATE RECORD #12:0", "truncate record #12:0", "TRUNCATE RECORD [#12:0]",
        "TRUNCATE RECORD [ #12:0 ]", "TRUNCATE RECORD [ #12:0, #12:1, #12:2 ]" })
      assertThatThrownBy(() -> new SQLAntlrParser(null).parse(query)).as(query).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining(SQLASTBuilder.TRUNCATE_RECORD_NOT_SUPPORTED);
  }

  @Test
  void malformedIsStillASyntaxError() {
    checkWrongSyntax("TRUNCATE RECORD");
    checkWrongSyntax("TRUNCATE RECORD #12:0, #12:1, #12:2 ");
  }
}
