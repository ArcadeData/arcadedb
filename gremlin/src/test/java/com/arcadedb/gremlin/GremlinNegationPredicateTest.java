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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5192: valid Gremlin negation predicates {@code has('id', neq(1))} and
 * {@code has('id', not(eq(1)))} must filter correctly instead of failing with an execution error that the
 * HTTP layer degrades to a 500 "Error on transaction commit".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinNegationPredicateTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setUp() {
    graph = ArcadeGraph.open("./target/testgremlinnegation");
    graph.getDatabase().getSchema().createVertexType("A");
    graph.getDatabase().transaction(() -> {
      graph.getDatabase().newVertex("A").set("id", 1).save();
      graph.getDatabase().newVertex("A").set("id", 2).save();
    });
  }

  @AfterEach
  void tearDown() {
    if (graph != null) {
      graph.drop();
      FileUtils.deleteRecursively(new File("./target/testgremlinnegation"));
    }
  }

  @Test
  void neqPredicateInHas() {
    assertThat(execute("g.V().hasLabel('A').has('id', neq(1)).values('id').fold()")).containsExactly(2);
  }

  @Test
  void notEqPredicateInHas() {
    assertThat(execute("g.V().hasLabel('A').has('id', not(eq(1))).values('id').fold()")).containsExactly(2);
  }

  @Test
  void neqPredicateWithoutLabel() {
    assertThat(execute("g.V().has('id', neq(1)).values('id').fold()")).containsExactly(2);
  }

  private List<Object> execute(final String query) {
    final ResultSet resultSet = graph.gremlin(query).execute();
    final List<Object> values = new ArrayList<>();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      final Object value = row.getProperty("result");
      if (value instanceof List<?> list)
        values.addAll(list);
      else
        values.add(value);
    }
    return values;
  }
}
