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
package com.arcadedb.function.polyglot;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionExecutionException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5121:
 * - Part A: DEFINE FUNCTION libraries must survive a server restart (persisted in schema.json).
 * - Part B: a failed DEFINE FUNCTION must not poison the whole library.
 */
class SQLDefinedFunctionPersistenceTest extends TestHelper {

  // ---------------------------------------------------------------- Part A: persistence across restart

  @Test
  void jsFunctionSurvivesRestart() {
    database.command("sql", "define function t.f1 \"return 1;\" language js");
    database.command("sql", "define function math.sum \"return a + b;\" parameters [a,b] language js");

    assertThat((Integer) database.getSchema().getFunction("t", "f1").execute()).isEqualTo(1);
    assertThat((Integer) database.getSchema().getFunction("math", "sum").execute(3, 5)).isEqualTo(8);

    reopenDatabase();

    // Function libraries must be reconstructed from schema.json after the restart.
    assertThat(database.getSchema().hasFunctionLibrary("t")).isTrue();
    assertThat(database.getSchema().hasFunctionLibrary("math")).isTrue();

    assertThat((Integer) database.getSchema().getFunction("t", "f1").execute()).isEqualTo(1);
    assertThat((Integer) database.getSchema().getFunction("math", "sum").execute(10, 20)).isEqualTo(30);

    // and callable from SQL too
    assertThat((Integer) database.command("sql", "select `math.sum`(?,?) as result", 4, 6).next().getProperty("result"))
        .isEqualTo(10);
  }

  @Test
  void sqlFunctionSurvivesRestart() {
    database.command("sql", "define function util.double \"select :n * 2 as r\" parameters [n] language sql");
    assertThat(((Number) database.getSchema().getFunction("util", "double").execute(21)).intValue()).isEqualTo(42);

    reopenDatabase();

    assertThat(database.getSchema().hasFunctionLibrary("util")).isTrue();
    assertThat(((Number) database.getSchema().getFunction("util", "double").execute(21)).intValue()).isEqualTo(42);
  }

  @Test
  void deletedFunctionStaysDeletedAfterRestart() {
    database.command("sql", "define function t.f1 \"return 1;\" language js");
    database.command("sql", "define function t.f2 \"return 2;\" language js");

    database.command("sql", "delete function t.f2");
    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("f2")).isFalse();

    reopenDatabase();

    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("f1")).isTrue();
    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("f2")).isFalse();
    assertThat((Integer) database.getSchema().getFunction("t", "f1").execute()).isEqualTo(1);
  }

  // ---------------------------------------------------------------- Part B: a broken definition must not poison the library

  @Test
  void brokenDefinitionDoesNotPoisonLibrary() {
    database.command("sql", "define function t.f1 \"return 1;\" language js");
    assertThat((Integer) database.getSchema().getFunction("t", "f1").execute()).isEqualTo(1);

    // Invalid JS body: unbalanced parenthesis -> compilation error.
    assertThatThrownBy(() -> database.command("sql", "define function t.bad \"return (\" language js"))
        .isInstanceOf(FunctionExecutionException.class);

    // The broken function must NOT be registered in the library.
    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("bad")).isFalse();

    // Defining another VALID function must still work (library not poisoned).
    database.command("sql", "define function t.good \"return 2;\" language js");
    assertThat((Integer) database.getSchema().getFunction("t", "good").execute()).isEqualTo(2);

    // The previously working function must still be callable (no NPE from a null library reference).
    assertThat((Integer) database.getSchema().getFunction("t", "f1").execute()).isEqualTo(1);
  }

  // ---------------------------------------------------------------- Escaped characters in the body (issue #5121)

  @Test
  void bodyWithEscapedQuotesAndNewlines() {
    // \" must become a real double quote and \n a real newline inside the stored body.
    database.command("sql", "define function t.q \"var s = \\\"x\\\";\\nreturn s;\" language js");
    assertThat(database.getSchema().getFunction("t", "q").execute()).isEqualTo("x");

    // The escapes must survive a restart too (persisted body is the decoded form, re-declared cleanly).
    reopenDatabase();
    assertThat(database.getSchema().getFunction("t", "q").execute()).isEqualTo("x");
  }

  @Test
  void bodyWithBackslash() {
    // SQL \\\\ (4 backslashes) decodes to \\ (2), so the JS body becomes `return '\\'.charCodeAt(0);`
    // i.e. a single backslash char whose code point is 92.
    database.command("sql", "define function t.bs \"return '\\\\\\\\'.charCodeAt(0);\" language js");
    assertThat((Integer) database.getSchema().getFunction("t", "bs").execute()).isEqualTo(92);

    reopenDatabase();
    assertThat((Integer) database.getSchema().getFunction("t", "bs").execute()).isEqualTo(92);
  }

  @Test
  void brokenDefinitionIsNotPersisted() {
    database.command("sql", "define function t.f1 \"return 1;\" language js");
    assertThatThrownBy(() -> database.command("sql", "define function t.bad \"return (\" language js"))
        .isInstanceOf(FunctionExecutionException.class);
    database.command("sql", "define function t.good \"return 2;\" language js");

    reopenDatabase();

    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("f1")).isTrue();
    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("good")).isTrue();
    assertThat(database.getSchema().getFunctionLibrary("t").hasFunction("bad")).isFalse();
    assertThat((Integer) database.getSchema().getFunction("t", "f1").execute()).isEqualTo(1);
    assertThat((Integer) database.getSchema().getFunction("t", "good").execute()).isEqualTo(2);
  }
}
