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
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLDefinedSQLFunctionTest extends TestHelper {
  @Test
  void embeddedFunction() {
    registerFunctions();
    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void callFromSQLWithParams() {
    registerFunctions();
    final ResultSet result = database.command("sql", "select `math.sum`(?,?) as result", 3, 5);
    assertThat((Integer) result.next().getProperty("result")).isEqualTo(8);
  }

  @Test
  void callFromSQLNoParams() {
    database.command("sql", "define function math.hello \"select 'hello'\" language sql");
    final ResultSet result = database.command("sql", "select `math.hello`() as result");
    assertThat(result.next().<String>getProperty("result")).isEqualTo("hello");
  }

  @Test
  void errorTestCallFromSQLEmptyParams() {
    assertThatThrownBy(() -> database.command("sql", "define function math.hello \"select 'hello'\" parameters [] language sql")).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void reuseSameQueryEngine() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("util", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);

    result = (Integer) database.getSchema().getFunction("util", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  void redefineFunction() {
    registerFunctions();

    Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(100, 50);
    assertThat(result).isEqualTo(150);

    assertThatThrownBy(() -> database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql")).isInstanceOf(IllegalArgumentException.class);

    database.getSchema().getFunctionLibrary("math").unregisterFunction("sum");
    database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");

    result = (Integer) database.getSchema().getFunction("math", "sum").execute(-350, 150);
    assertThat(result).isEqualTo(-200);
  }

  private void registerFunctions() {
    database.command("sql", "define function math.sum \"select :a + :b;\" parameters [a,b] language sql");
    database.command("sql", "define function util.sum \"select :a + :b;\" parameters [a,b] language sql");

    final FunctionLibraryDefinition flib = database.getSchema().getFunctionLibrary("math");
    assertThat(flib).isNotNull();
  }
}
