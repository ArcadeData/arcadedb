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
package com.arcadedb.function.sql.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces issue #6826: {@code SQLFunctionIf.getSyntax()} documents the third argument as optional, but the body
 * read {@code params[2]} unconditionally and the class declared no arity. The documented two-argument form therefore
 * answered {@code null} and wrote an ArrayIndexOutOfBoundsException stack trace at SEVERE <em>once per evaluated
 * row</em>, and {@code if()} / {@code if(true)} did the same instead of reporting a clean syntax error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6826FunctionIfArityTest extends TestHelper {

  @Test
  void twoArgumentFormReturnsNullOnTheFalseBranch() {
    database.getSchema().createDocumentType("Doc6826");
    database.transaction(() -> database.newDocument("Doc6826").set("rich", false).save());

    try (final ResultSet rs = database.query("sql", "select if(rich, 'yes') as v from Doc6826")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("v")).isNull();
    }
  }

  @Test
  void twoArgumentFormReturnsTheValueOnTheTrueBranch() {
    database.getSchema().createDocumentType("Doc6826b");
    database.transaction(() -> database.newDocument("Doc6826b").set("rich", true).save());

    try (final ResultSet rs = database.query("sql", "select if(rich, 'yes') as v from Doc6826b")) {
      assertThat(rs.next().<String>getProperty("v")).isEqualTo("yes");
    }
  }

  @Test
  void threeArgumentFormIsUnchanged() {
    database.getSchema().createDocumentType("Doc6826c");
    database.transaction(() -> database.newDocument("Doc6826c").set("rich", false).save());

    try (final ResultSet rs = database.query("sql", "select if(rich, 'rich', 'poor') as v from Doc6826c")) {
      assertThat(rs.next().<String>getProperty("v")).isEqualTo("poor");
    }
  }

  @Test
  void tooFewArgumentsIsACleanSyntaxError() {
    database.getSchema().createDocumentType("Doc6826d");
    database.transaction(() -> database.newDocument("Doc6826d").set("rich", true).save());

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "select if(rich) as v from Doc6826d")) {
        rs.next();
      }
    }).isInstanceOf(CommandSemanticException.class).hasMessageContaining("2-3 arguments");
  }

  @Test
  void tooManyArgumentsIsACleanSyntaxError() {
    database.getSchema().createDocumentType("Doc6826e");
    database.transaction(() -> database.newDocument("Doc6826e").set("rich", true).save());

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "select if(rich, 1, 2, 3) as v from Doc6826e")) {
        rs.next();
      }
    }).isInstanceOf(CommandSemanticException.class).hasMessageContaining("2-3 arguments");
  }

  @Test
  void arityIsDeclared() {
    final SQLFunctionIf function = new SQLFunctionIf();
    assertThat(function.getMinArgs()).isEqualTo(2);
    assertThat(function.getMaxArgs()).isEqualTo(3);
  }

  @Test
  void anErrorRaisedByTheChosenBranchIsNotSwallowed() {
    final SQLFunctionIf function = new SQLFunctionIf();
    assertThat(function.execute(null, null, null, new Object[] { true, "yes" }, null)).isEqualTo("yes");
    assertThat(function.execute(null, null, null, new Object[] { false, "yes" }, null)).isNull();
    assertThat(function.execute(null, null, null, new Object[] { false, "yes", "no" }, null)).isEqualTo("no");
    // A condition of an unsupported kind stays a null, as before.
    assertThat(function.execute(null, null, null, new Object[] { new Object(), "yes", "no" }, null)).isNull();
  }
}
