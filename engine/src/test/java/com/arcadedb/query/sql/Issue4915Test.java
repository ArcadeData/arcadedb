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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #4915: map string-key indexing ($map["key"]) returned null inside
 * INSERT ... CONTENT while it worked with INSERT ... SET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4915Test extends TestHelper {
  @Test
  void contentBracketNotationWithStringKey() {
    database.command("sql", "CREATE DOCUMENT TYPE yolo");
    final String script = """
        begin;
        LET $test = {"name":"1","value":"2"};
        LET $inserted = INSERT INTO yolo CONTENT {key:"name",value:$test["name"]};
        commit;
        return $inserted;
        """;
    final ResultSet rs = database.command("sqlscript", script);
    final Result r = rs.next();
    assertThat(r.<String>getProperty("value")).isEqualTo("1");
    assertThat(r.<String>getProperty("key")).isEqualTo("name");
  }

  @Test
  void setBracketNotationWithStringKey() {
    database.command("sql", "CREATE DOCUMENT TYPE yolo");
    final String script = """
        begin;
        LET $test = {"name":"1","value":"2"};
        LET $inserted = INSERT INTO yolo SET key="name",value=$test["name"];
        commit;
        return $inserted;
        """;
    final ResultSet rs = database.command("sqlscript", script);
    final Result r = rs.next();
    assertThat(r.<String>getProperty("value")).isEqualTo("1");
    assertThat(r.<String>getProperty("key")).isEqualTo("name");
  }

  @Test
  void contentNumericIndexStillWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE yolo");
    final String script = """
        begin;
        LET $test = ["a","b","c"];
        LET $inserted = INSERT INTO yolo CONTENT {value:$test[1]};
        commit;
        return $inserted;
        """;
    final ResultSet rs = database.command("sqlscript", script);
    final Result r = rs.next();
    assertThat(r.<String>getProperty("value")).isEqualTo("b");
  }
}
