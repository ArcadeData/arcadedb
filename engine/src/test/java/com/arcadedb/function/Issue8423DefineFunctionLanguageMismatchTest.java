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
package com.arcadedb.function;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8423: {@code DEFINE FUNCTION lib.f ... LANGUAGE x} against an existing library of another language built the
 * function from the statement's language and handed it to the library without comparing the two, which surfaced as a
 * raw {@link ClassCastException}. It must be refused with a message naming both languages, and leave the library as it
 * was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8423DefineFunctionLanguageMismatchTest extends TestHelper {

  @Test
  void sqlFunctionIntoJsLibraryIsRefused() {
    database.command("sql", "DEFINE FUNCTION lib.twice 'return x * 2' PARAMETERS [x] LANGUAGE js");

    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION lib.f 'SELECT 1 AS result' LANGUAGE sql"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("'sql' function")
        .hasMessageContaining("'js' library");

    assertLibraryUnchanged("lib", "twice");
    assertThat(callFunction("SELECT `lib.twice`(21) AS result")).isEqualTo(42);
  }

  @Test
  void jsFunctionIntoSqlLibraryIsRefused() {
    database.command("sql", "DEFINE FUNCTION sqllib.one 'SELECT 1 AS result' LANGUAGE sql");

    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION sqllib.f 'return 1' LANGUAGE js"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("'js' function")
        .hasMessageContaining("'sql' library");

    assertLibraryUnchanged("sqllib", "one");
  }

  @Test
  void cypherFunctionIntoSqlLibraryIsRefused() {
    database.command("sql", "DEFINE FUNCTION mixed.one 'SELECT 1 AS result' LANGUAGE sql");

    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION mixed.f 'RETURN 1' LANGUAGE opencypher"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("'sql' library");

    assertLibraryUnchanged("mixed", "one");
  }

  @Test
  void cypherAliasJoinsOpenCypherLibrary() {
    // 'cypher' IS AN ALIAS OF 'opencypher', THE LANGUAGE THE LIBRARY REPORTS: THE TWO MUST NOT BE TOLD APART
    database.command("sql", "DEFINE FUNCTION cy.double 'RETURN $x * 2' PARAMETERS [x] LANGUAGE opencypher");
    database.command("sql", "DEFINE FUNCTION cy.triple 'RETURN $x * 3' PARAMETERS [x] LANGUAGE cypher");

    assertThat(database.getSchema().getFunctionLibrary("cy").hasFunction("double")).isTrue();
    assertThat(database.getSchema().getFunctionLibrary("cy").hasFunction("triple")).isTrue();
  }

  @Test
  void languageIsCaseInsensitive() {
    database.command("sql", "DEFINE FUNCTION upper.twice 'return x * 2' PARAMETERS [x] LANGUAGE JS");
    database.command("sql", "DEFINE FUNCTION upper.thrice 'return x * 3' PARAMETERS [x] LANGUAGE Js");
    database.command("sql", "DEFINE FUNCTION upcy.double 'RETURN $x * 2' PARAMETERS [x] LANGUAGE CYPHER");

    assertThat(database.getSchema().getFunctionLibrary("upper").getLanguage()).isEqualTo("js");
    assertThat(database.getSchema().getFunctionLibrary("upcy").getLanguage()).isEqualTo("opencypher");
    assertThat(callFunction("SELECT `upper.thrice`(3) AS result")).isEqualTo(9);

    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION upper.f 'SELECT 1 AS result' LANGUAGE SQL"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("'js' library");
  }

  @Test
  void unsupportedLanguageIntoExistingLibraryIsRefused() {
    database.command("sql", "DEFINE FUNCTION lib2.one 'SELECT 1 AS result' LANGUAGE sql");

    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION lib2.f 'x' LANGUAGE python"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("language 'python' not supported");

    assertLibraryUnchanged("lib2", "one");
  }

  @Test
  void javaLibraryIsRefused() throws Exception {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("javalib", Math.class));

    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION javalib.f 'SELECT 1 AS result' LANGUAGE sql"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("not a user-defined function library");

    assertThat(database.getSchema().getFunctionLibrary("javalib").hasFunction("f")).isFalse();
  }

  @Test
  void missingLanguageIsAParseErrorNotANullPointer() {
    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION nolang.f 'SELECT 1 AS result'"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("requires a LANGUAGE");

    assertThat(database.getSchema().hasFunctionLibrary("nolang")).isFalse();
  }

  @Test
  void refusedDefinitionIsNotPersisted() {
    database.command("sql", "DEFINE FUNCTION persisted.twice 'return x * 2' PARAMETERS [x] LANGUAGE js");
    assertThatThrownBy(() -> database.command("sql", "DEFINE FUNCTION persisted.f 'SELECT 1 AS result' LANGUAGE sql"))
        .isInstanceOf(CommandSQLParsingException.class);

    reopenDatabase();

    assertLibraryUnchanged("persisted", "twice");
    assertThat(callFunction("SELECT `persisted.twice`(4) AS result")).isEqualTo(8);
  }

  private void assertLibraryUnchanged(final String libraryName, final String onlyFunction) {
    final FunctionLibraryDefinition<?> library = database.getSchema().getFunctionLibrary(libraryName);
    int count = 0;
    for (final FunctionDefinition f : library.getFunctions()) {
      assertThat(f.getName()).isEqualTo(onlyFunction);
      ++count;
    }
    assertThat(count).isEqualTo(1);
  }

  private int callFunction(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return ((Number) rs.next().getProperty("result")).intValue();
    }
  }
}
