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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10770: the value of an eager terminal step ({@code next()}, {@code toList()}, ...) was wrapped into the result with
 * {@code Set.of(value)}, which refuses null, so a traversal whose terminal step legitimately produced null failed with a
 * {@code NullPointerException} while the same traversal without the terminal step answered one row holding null. The value of a
 * {@code tryNext()} was handed back as an {@code Optional} object instead of as the value it holds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10770GremlinNullTerminalResultTest {
  private static final String PATH = "./target/test-gremlin-10770-null-terminal";

  private ArcadeGraph graph;
  private Database    database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    database = (Database) graph.getDatabase();
    database.command("sql", "CREATE VERTEX TYPE V1").close();
    database.transaction(() -> database.newVertex("V1").set("name", "a").save());
  }

  @AfterEach
  void teardown() {
    if (graph != null) {
      if (graph.tx().isOpen())
        graph.tx().rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  void aNullReachedByATerminalStepIsTheRowTheTraversalFormAnswers() {
    final String traversal = "g.V().hasLabel('V1').coalesce(__.values('missing'), __.constant(null))";

    // THE CONTROL: THE SAME VALUE WITHOUT A TERMINAL STEP IS ONE ROW HOLDING NULL
    assertThat(results(traversal)).containsExactly((Object) null);

    assertThat(results(traversal + ".next()")).containsExactly((Object) null);
    assertThat(results("g.V().hasLabel('V1').constant(null).next()")).containsExactly((Object) null);
    assertThat(results("g.inject(null).next()")).containsExactly((Object) null);
  }

  @Test
  void aListHoldingNullIsKeptAsTheValueOfTheTerminalStep() {
    assertThat(results("g.inject(1, null, 3).toList()")).containsExactly(Arrays.asList(1, null, 3));
    assertThat(results("g.inject(null, 2, 3).next(2)")).containsExactly(Arrays.asList(null, 2));
  }

  @Test
  void tryNextAnswersTheValueItHoldsOrNoRow() {
    assertThat(results("g.V().hasLabel('V1').values('name').tryNext()")).containsExactly("a");
    assertThat(results("g.V().hasLabel('V1').values('missing').tryNext()")).isEmpty();
  }

  @Test
  void aScalarTerminalValueIsStillOneRow() {
    assertThat(results("g.V().hasLabel('V1').values('name').next()")).containsExactly("a");
    assertThat(results("g.V().hasLabel('V1').hasNext()")).containsExactly(true);
  }

  private List<Object> results(final String query) {
    final List<Object> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = graph.gremlin(query).execute()) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty("result"));
        }
      }
    });
    return values;
  }
}
