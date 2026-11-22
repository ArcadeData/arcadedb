/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.function.cypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Name resolution of user-defined functions from Cypher: a library and a function match in any letter case, the exact spelling
 * first, a missing one is a client error naming the function as written, and SQL, which keeps the exact spelling, reaches the
 * same function through its quoted name.
 */
class CustomFunctionAdapterResolutionTest extends TestHelper {

  private BasicCommandContext context() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private Object cypher(final String query) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }

  @Test
  void aLibraryAndAFunctionMatchInAnyLetterCaseButTheExactSpellingWins() {
    database.command("sql", "DEFINE FUNCTION MixedLib.MixedFn 'SELECT 1 AS result' LANGUAGE sql");

    final FunctionDefinition exact = CustomFunctionAdapter.resolve(database.getSchema(), "MixedLib", "MixedFn");
    assertThat(exact).isNotNull();
    assertThat(exact.getName()).isEqualTo("MixedFn");
    for (final String[] spelling : new String[][] { { "mixedlib", "mixedfn" }, { "MIXEDLIB", "MIXEDFN" }, { "mixedLIB", "MixedFn" },
        { "MixedLib", "mixedfn" } })
      assertThat(CustomFunctionAdapter.resolve(database.getSchema(), spelling[0], spelling[1])).as(spelling[0] + "." + spelling[1])
          .isSameAs(exact);

    assertThat(CustomFunctionAdapter.resolve(database.getSchema(), "MixedLib", "other")).isNull();
    assertThat(CustomFunctionAdapter.resolve(database.getSchema(), "nolib", "MixedFn")).isNull();

    // two functions that differ only by case: each exact spelling reaches its own, any other spelling reaches one of them
    database.command("sql", "DEFINE FUNCTION MixedLib.mixedfn 'SELECT 2 AS result' LANGUAGE sql");
    assertThat(CustomFunctionAdapter.resolve(database.getSchema(), "MixedLib", "MixedFn").getName()).isEqualTo("MixedFn");
    assertThat(CustomFunctionAdapter.resolve(database.getSchema(), "MixedLib", "mixedfn").getName()).isEqualTo("mixedfn");
    assertThat(CustomFunctionAdapter.resolve(database.getSchema(), "MixedLib", "MIXEDFN")).isNotNull();
  }

  @Test
  void existsReadsALibraryThatIsItselfDottedAndRefusesWhatHasNoLibraryOrNoFunctionName() {
    database.command("sql", "DEFINE FUNCTION existslib.fn 'SELECT 1 AS result' LANGUAGE sql");
    final var schema = database.getSchema();

    assertThat(CustomFunctionAdapter.exists(schema, "existslib.fn")).isTrue();
    assertThat(CustomFunctionAdapter.exists(schema, "EXISTSLIB.FN")).isTrue();
    assertThat(CustomFunctionAdapter.exists(schema, "existslib.nothing")).isFalse();
    assertThat(CustomFunctionAdapter.exists(schema, "nolib.fn")).isFalse();
    assertThat(CustomFunctionAdapter.exists(schema, "fn")).isFalse();
    assertThat(CustomFunctionAdapter.exists(schema, "")).isFalse();
    // a name with no library before the dot or no function after it
    assertThat(CustomFunctionAdapter.exists(schema, ".fn")).isFalse();
    assertThat(CustomFunctionAdapter.exists(schema, "existslib.")).isFalse();
    assertThat(CustomFunctionAdapter.exists(schema, ".")).isFalse();
    // the split is at the LAST dot: a library name may hold dots, a function name may not
    assertThat(CustomFunctionAdapter.exists(schema, "existslib.fn.x")).isFalse();
    assertThat(CustomFunctionAdapter.exists(schema, "a.b")).isFalse();
    // one character each side of the dot is a name
    database.command("sql", "DEFINE FUNCTION x.y 'SELECT 1 AS result' LANGUAGE sql");
    assertThat(CustomFunctionAdapter.exists(schema, "x.y")).isTrue();
    assertThat(CustomFunctionAdapter.exists(schema, "X.Y")).isTrue();
  }

  @Test
  void aFunctionDroppedAfterTheAdapterWasBuiltIsAClientErrorNamingItAsWritten() {
    database.command("sql", "DEFINE FUNCTION droplib.fn 'SELECT 7 AS result' LANGUAGE sql");
    final CustomFunctionAdapter adapter = new CustomFunctionAdapter("droplib", "fn");
    assertThat(adapter.getName()).isEqualTo("droplib.fn");
    assertThat(adapter.execute(new Object[0], context())).isEqualTo(7);

    database.command("sql", "DELETE FUNCTION droplib.fn");
    assertThatThrownBy(() -> adapter.execute(new Object[0], context())).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Unknown function 'droplib.fn'");
  }

  @Test
  void aFunctionRedefinedAfterTheAdapterWasBuiltRunsItsNewBody() {
    database.command("sql", "DEFINE FUNCTION redeflib.fn 'SELECT 1 AS result' LANGUAGE sql");
    final CustomFunctionAdapter adapter = new CustomFunctionAdapter("redeflib", "fn");
    assertThat(adapter.execute(new Object[0], context())).isEqualTo(1);

    database.command("sql", "DELETE FUNCTION redeflib.fn");
    database.command("sql", "DEFINE FUNCTION redeflib.fn 'SELECT 2 AS result' LANGUAGE sql");
    assertThat(adapter.execute(new Object[0], context())).isEqualTo(2);
  }

  @Test
  void cypherCallsAUserFunctionInAnyCaseAndPassesItsArgumentsAndAnswerThrough() {
    database.command("sql", "DEFINE FUNCTION CaseLib.AddOne 'SELECT :a + 1 AS result' PARAMETERS [a] LANGUAGE sql");

    assertThat(cypher("RETURN CaseLib.AddOne(41) AS v")).isEqualTo(42L);
    assertThat(cypher("RETURN caselib.addone(41) AS v")).isEqualTo(42L);
    assertThat(cypher("RETURN CASELIB.ADDONE(41) AS v")).isEqualTo(42L);
    assertThat(cypher("RETURN CaseLib.AddOne(null) AS v")).isNull();
    assertThatThrownBy(() -> cypher("RETURN CaseLib.Nothing(1) AS v")).hasMessageContaining("Unknown function");

    // SQL keeps the exact spelling of a user function, quoted as a whole
    try (final ResultSet rs = database.command("sql", "SELECT `CaseLib.AddOne`(1) AS v")) {
      assertThat(((Number) rs.next().getProperty("v")).longValue()).isEqualTo(2L);
    }
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sql", "SELECT `caselib.addone`(1) AS v")) {
        rs.next();
      }
    }).hasMessageContaining("Unknown function");
  }

  @Test
  void aFunctionAnsweringAListReachesCypherAsOneValue() {
    database.command("sql", "DEFINE FUNCTION listlib.pair 'return [a, b]' PARAMETERS [a, b] LANGUAGE js");
    assertThat(cypher("RETURN listlib.pair(1, 2) AS v")).isEqualTo(List.of(1, 2));
  }
}
