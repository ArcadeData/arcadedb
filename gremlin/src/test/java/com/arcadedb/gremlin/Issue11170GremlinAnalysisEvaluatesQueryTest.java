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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11170: a caller that uses {@code analyze()} as a check made before a query runs (the MCP tools do) must know that the
 * Gremlin engine builds the traversal by evaluating the text, so it is not such a check, while the parsing engines are.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11170GremlinAnalysisEvaluatesQueryTest {
  private static final String DB_PATH = "./target/testgremlin11170";

  @Test
  void gremlinAnalysisEvaluatesTheQueryWhileParsingEnginesDoNot() {
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    try {
      final Database database = (Database) graph.getDatabase();
      assertThat(database.getQueryEngine("gremlin").analysisEvaluatesQuery()).isTrue();
      assertThat(database.getQueryEngine("sql").analysisEvaluatesQuery()).isFalse();
      assertThat(database.getQueryEngine("cypher").analysisEvaluatesQuery()).isFalse();
    } finally {
      graph.getDatabase().drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
