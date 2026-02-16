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
package com.arcadedb.function.sql.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLFunctionConcatTest {
  private SQLFunctionConcat function;

  @BeforeEach
  void setup() {
    function = new SQLFunctionConcat();
  }

  @Test
  void concatSingleField() {
    function.execute(null, null, null, new Object[] { "Hello" }, null);
    function.execute(null, null, null, new Object[] { "World" }, null);
    Object result = function.getResult();
    assertThat(result).isEqualTo("HelloWorld");
  }

  @Test
  void concatWithDelimiter() {
    function.execute(null, null, null, new Object[] { "Hello", " " }, null);
    function.execute(null, null, null, new Object[] { "World", " " }, null);
    Object result = function.getResult();
    assertThat(result).isEqualTo("Hello World");
  }

  @Test
  void concatEmpty() {
    Object result = function.getResult();
    assertThat(result).isNull();
  }

  @Test
  void concatWithNullValues() {
    function.execute(null, null, null, new Object[] { null, " " }, null);
    function.execute(null, null, null, new Object[] { "World", " " }, null);
    Object result = function.getResult();
    assertThat(result).isEqualTo("null World");
  }

  @Test
  void query() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionConcat", (db) -> {
      setUpDatabase(db);
      ResultSet result = db.query("sql", "select concat(name, ' ') as concat from Person");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<String>getProperty("concat")).isEqualTo("Alan Brian");
    });
  }

  private void setUpDatabase(Database db) {
    db.command("sql", "create document type Person");
    db.command("sql", "insert into Person set name = 'Alan'");
    db.command("sql", "insert into Person set name = 'Brian'");
  }
}
