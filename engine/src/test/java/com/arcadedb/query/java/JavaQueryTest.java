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
package com.arcadedb.query.java;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.*;

class JavaMethods {
  public JavaMethods() {
  }

  public int sum(final int a, final int b) {
    return a + b;
  }

  public static int SUM(final int a, final int b) {
    return a + b;
  }

  public static void hello() {
    // EMPTY METHOD
  }
}

class JavaQueryTest extends TestHelper {
  @Test
  void registeredMethod() {
    assertThat(database.getQueryEngine("java").getLanguage()).isEqualTo("java");

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");

    final ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);
  }

  @Test
  void registeredMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::sum");
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods::SUM");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  void registeredClass() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");

    ResultSet result = database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    result = database.command("java", "com.arcadedb.query.java.JavaMethods::SUM", 5, 3);
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);

    database.getQueryEngine("java").unregisterFunctions();
  }

  @Test
  void unRegisteredMethod() {
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3);
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
      assertThat(e.getCause() instanceof SecurityException).isTrue();
    }
  }

  @Test
  void notExistentMethod() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    try {
      database.command("java", "com.arcadedb.query.java.JavaMethods::totallyInvented", 5, 3);
      fail("");
    } catch (final CommandExecutionException e) {
      // EXPECTED
      assertThat(e.getCause() instanceof NoSuchMethodException).isTrue();
    }
  }

  @Test
  void analyzeQuery() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("java").analyze("com.arcadedb.query.java.JavaMethods::totallyInvented");
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.isIdempotent()).isFalse();
  }

  @Test
  void unsupportedMethods() {
    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    assertThatThrownBy(() -> database.query("java", "com.arcadedb.query.java.JavaMethods::sum", 5, 3)).isInstanceOf(UnsupportedOperationException.class);

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    assertThatThrownBy(() -> {
      final HashMap map = new HashMap();
      map.put("name", 1);
      database.getQueryEngine("java").command("com.arcadedb.query.java.JavaMethods::hello", null, map);
    }).isInstanceOf(UnsupportedOperationException.class);

    database.getQueryEngine("java").registerFunctions("com.arcadedb.query.java.JavaMethods");
    assertThatThrownBy(() -> database.getQueryEngine("java").query("com.arcadedb.query.java.JavaMethods::sum", null, new HashMap<>())).isInstanceOf(UnsupportedOperationException.class);
  }
}
