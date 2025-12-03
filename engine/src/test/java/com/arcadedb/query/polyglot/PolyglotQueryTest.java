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
package com.arcadedb.query.polyglot;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.graalvm.polyglot.PolyglotException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class PolyglotQueryTest extends TestHelper {
  @Test
  void sum() {
    final ResultSet result = database.command("js", "3 + 5");
    assertThat(result.hasNext()).isTrue();
    assertThat((Integer) result.next().getProperty("value")).isEqualTo(8);
  }

  @Test
  void databaseQuery() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Product");
      database.newVertex("Product").set("name", "Amiga 1200", "price", 900).save();
    });

    final ResultSet result = database.command("js", "database.query('sql', 'select from Product')");
    assertThat(result.hasNext()).isTrue();

    final Vertex vertex = result.next().getRecord().get().asVertex();
    assertThat(vertex.get("name")).isEqualTo("Amiga 1200");
    assertThat(vertex.get("price")).isEqualTo(900);
  }

  @Test
  void sandbox() {
    // BY DEFAULT NO JAVA PACKAGES ARE ACCESSIBLE
    try {
      final ResultSet result = database.command("js", "let BigDecimal = Java.type('java.math.BigDecimal'); new BigDecimal(1)");
      assertThat(result.hasNext()).isFalse();
      fail("It should not execute the function");
    } catch (final Exception e) {
      assertThat(e instanceof CommandExecutionException).isTrue();
      assertThat(e.getCause() instanceof PolyglotException).isTrue();
      assertThat(e.getCause().getMessage().contains("java.math.BigDecimal")).isTrue();
    }

    // ALLOW ACCESSING TO BIG DECIMAL CLASS
    ((LocalDatabase) database).registerReusableQueryEngine(
        new PolyglotQueryEngine.PolyglotQueryEngineFactory("js").setAllowedPackages(List.of("java.math.BigDecimal"))
            .getInstance((DatabaseInternal) database));

    final ResultSet result = database.command("js", "let BigDecimal = Java.type('java.math.BigDecimal'); new BigDecimal(1)");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<BigDecimal>getProperty("value")).isEqualTo(new BigDecimal(1));
  }

  @Test
  void sandboxSystem() {
    // BY DEFAULT NO JAVA PACKAGES ARE ACCESSIBLE
    try {
      final ResultSet result = database.command("js", "let System = Java.type('java.lang.System'); System.exit(1)");
      assertThat(result.hasNext()).isFalse();
      fail("It should not execute the function");
    } catch (final Exception e) {
      assertThat(e instanceof CommandExecutionException).isTrue();
      assertThat(e.getCause() instanceof PolyglotException).isTrue();
      assertThat(e.getCause().getMessage().contains("java.lang.System")).isTrue();
    }
  }

  @Test
  void timeout() {
    GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT.setValue(2000);
    try {
      database.command("js", "while(true);");
      fail("It should go in timeout");
    } catch (final Exception e) {
      assertThat(e instanceof CommandExecutionException).isTrue();
      assertThat(e.getCause() instanceof TimeoutException).isTrue();
    } finally {
      GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT.reset();
    }
  }

  @Test
  void analyzeQuery() {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("js").analyze("3 + 5");
    assertThat(analyzed.isDDL()).isFalse();
    assertThat(analyzed.isIdempotent()).isFalse();
  }
}
