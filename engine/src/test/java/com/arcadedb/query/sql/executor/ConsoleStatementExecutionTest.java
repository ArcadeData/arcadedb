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
import com.arcadedb.exception.CommandExecutionException;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 */
class ConsoleStatementExecutionTest extends TestHelper {

  @Test
  void error() {
    ResultSet result = database.command("sqlscript", "console.`error` 'foo bar'");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("level")).isEqualTo("error");
    assertThat(item.<String>getProperty("message")).isEqualTo("foo bar");
  }

  @Test
  void log() {
    ResultSet result = database.command("sqlscript", "console.log 'foo bar'");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(result.hasNext()).isTrue();
    Result item = result.next();
    assertThat(item).isNotNull();
    assertThat(item.<String>getProperty("level")).isEqualTo("log");
    assertThat(item.<String>getProperty("message")).isEqualTo("foo bar");
  }

  @Test
  void logDocumentSerialization() {
    // Issue #3520: CONSOLE.log produces faulty serializations for documents
    database.command("sql", "CREATE DOCUMENT TYPE doc3520");
    database.begin();
    database.command("sql", "INSERT INTO doc3520 SET name = 'test'");
    database.commit();

    final ResultSet result = database.command("sqlscript",
        "LET $x = (SELECT FROM doc3520);\nCONSOLE.log $x");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final String msg = item.getProperty("message");
    // The message should contain the document properties as JSON, not internal debug format
    assertThat(msg).isNotNull();
    assertThat(msg).contains("\"name\"");
    assertThat(msg).contains("\"test\"");
    // Should not contain internal debug format with @type separator
    assertThat(msg).doesNotContain("@cat");
    assertThat(msg).doesNotContain("@type");
  }

  @Test
  void logMultiplePropertiesSerialization() {
    // Issue #3520: CONSOLE.log omits commas between multiple properties
    final ResultSet result = database.command("sqlscript",
        "LET $y = (SELECT 1, 'test');\nCONSOLE.log $y");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    final String msg = item.getProperty("message");
    // The message should contain commas between properties
    assertThat(msg).isNotNull();
    assertThat(msg).contains(", ");
  }

  @Test
  void letWithAndWithoutParenthesesProduceSameConsoleOutput() {
    // Issue #3519: LET $x = SELECT 1 vs LET $x = (SELECT 1) produce different CONSOLE.log output
    final ResultSet resultWithParens = database.command("sqlscript",
        "LET $x = (SELECT 1);\nCONSOLE.log $x");
    assertThat(resultWithParens.hasNext()).isTrue();
    final String msgWithParens = resultWithParens.next().getProperty("message");

    final ResultSet resultWithoutParens = database.command("sqlscript",
        "LET $x = SELECT 1;\nCONSOLE.log $x");
    assertThat(resultWithoutParens.hasNext()).isTrue();
    final String msgWithoutParens = resultWithoutParens.next().getProperty("message");

    // Both should produce the same output (the actual query result, not execution plan)
    assertThat(msgWithoutParens).isEqualTo(msgWithParens);
  }

  @Test
  void invalidLevel() {
    try {
      database.command("sqlscript", "console.bla 'foo bar'");
      fail("");
    } catch (CommandExecutionException x) {
      // EXPECTED
    } catch (Exception x2) {
      fail("");
    }
  }
}
