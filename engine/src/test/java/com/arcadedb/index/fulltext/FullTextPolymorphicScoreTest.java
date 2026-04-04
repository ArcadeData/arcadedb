/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for $score with polymorphic supertype full-text indexes.
 * Reproduces https://github.com/ArcadeData/arcadedb/issues/3776
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextPolymorphicScoreTest extends TestHelper {

  @Test
  void scoreOnPolymorphicSupertypeIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Searchable");
      database.command("sql", "CREATE PROPERTY Searchable.searchable_text STRING");

      database.command("sql", "CREATE VERTEX TYPE Decision EXTENDS Searchable");
      database.command("sql", "CREATE VERTEX TYPE Claim EXTENDS Searchable");

      database.command("sql", "CREATE INDEX ON Searchable (searchable_text) FULL_TEXT");

      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline hivemind scheduler agent'");
      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline'");
      database.command("sql",
          "INSERT INTO Claim SET searchable_text = 'hivemind scheduler agent controller'");
      database.command("sql",
          "INSERT INTO Claim SET searchable_text = 'dispatch'");
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT @type, searchable_text, $score AS score FROM Searchable"
              + " WHERE SEARCH_INDEX('Searchable[searchable_text]', 'dispatch pipeline hivemind scheduler agent') = true"
              + " LIMIT 10");

      final List<Float> scores = new ArrayList<>();
      while (result.hasNext()) {
        final Result r = result.next();
        final Float score = r.getProperty("score");
        assertThat(score).isNotNull();
        scores.add(score);
      }

      assertThat(scores).hasSize(4);

      // Different documents match different numbers of terms, so scores must vary
      assertThat(scores.stream().distinct().count())
          .as("Scores should not all be identical")
          .isGreaterThan(1);
    });
  }

  @Test
  void orderByScoreOnPolymorphicSupertypeIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Searchable");
      database.command("sql", "CREATE PROPERTY Searchable.searchable_text STRING");

      database.command("sql", "CREATE VERTEX TYPE Decision EXTENDS Searchable");
      database.command("sql", "CREATE VERTEX TYPE Claim EXTENDS Searchable");

      database.command("sql", "CREATE INDEX ON Searchable (searchable_text) FULL_TEXT");

      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline hivemind scheduler agent'");
      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline'");
      database.command("sql",
          "INSERT INTO Claim SET searchable_text = 'hivemind scheduler agent controller'");
      database.command("sql",
          "INSERT INTO Claim SET searchable_text = 'dispatch'");
    });

    database.transaction(() -> {
      // ORDER BY $score DESC must complete without hanging and return correct order
      final ResultSet result = database.query("sql",
          "SELECT @type, searchable_text, $score AS score FROM Searchable"
              + " WHERE SEARCH_INDEX('Searchable[searchable_text]', 'dispatch pipeline hivemind scheduler agent') = true"
              + " ORDER BY $score DESC LIMIT 5");

      final List<Float> scores = new ArrayList<>();
      final List<String> texts = new ArrayList<>();
      while (result.hasNext()) {
        final Result r = result.next();
        scores.add(r.getProperty("score"));
        texts.add(r.getProperty("searchable_text"));
      }

      assertThat(scores).hasSize(4);

      // Verify descending order
      for (int i = 0; i < scores.size() - 1; i++)
        assertThat(scores.get(i)).isGreaterThanOrEqualTo(scores.get(i + 1));

      // The document matching all 5 terms should be first
      assertThat(texts.get(0)).contains("dispatch pipeline hivemind scheduler agent");
    });
  }

  @Test
  void orderByScoreWithAdditionalFilter() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Searchable");
      database.command("sql", "CREATE PROPERTY Searchable.searchable_text STRING");
      database.command("sql", "CREATE PROPERTY Searchable.project_id INTEGER");

      database.command("sql", "CREATE VERTEX TYPE Decision EXTENDS Searchable");
      database.command("sql", "CREATE VERTEX TYPE Claim EXTENDS Searchable");

      database.command("sql", "CREATE INDEX ON Searchable (searchable_text) FULL_TEXT");

      // Project 1
      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline hivemind scheduler agent', project_id = 1");
      database.command("sql",
          "INSERT INTO Claim SET searchable_text = 'hivemind scheduler agent controller', project_id = 1");
      // Project 2 (should be excluded)
      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline', project_id = 2");
      database.command("sql",
          "INSERT INTO Claim SET searchable_text = 'dispatch', project_id = 2");
    });

    database.transaction(() -> {
      // Combines SEARCH_INDEX with an additional equality filter and ORDER BY
      final ResultSet result = database.query("sql",
          "SELECT @type, searchable_text, $score AS score FROM Searchable"
              + " WHERE SEARCH_INDEX('Searchable[searchable_text]', 'dispatch pipeline hivemind scheduler agent') = true"
              + "   AND project_id = 1"
              + " ORDER BY $score DESC LIMIT 5");

      final List<Float> scores = new ArrayList<>();
      while (result.hasNext()) {
        final Result r = result.next();
        scores.add(r.getProperty("score"));
      }

      // Only project_id = 1 records should be returned
      assertThat(scores).hasSize(2);

      // Descending order
      assertThat(scores.get(0)).isGreaterThanOrEqualTo(scores.get(1));
    });
  }

  @Test
  void usesIndexedFunctionFetchStep() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Searchable");
      database.command("sql", "CREATE PROPERTY Searchable.searchable_text STRING");
      database.command("sql", "CREATE VERTEX TYPE Decision EXTENDS Searchable");

      database.command("sql", "CREATE INDEX ON Searchable (searchable_text) FULL_TEXT");

      database.command("sql",
          "INSERT INTO Decision SET searchable_text = 'dispatch pipeline'");
    });

    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "EXPLAIN SELECT searchable_text, $score AS score FROM Searchable"
              + " WHERE SEARCH_INDEX('Searchable[searchable_text]', 'dispatch') = true"
              + " ORDER BY $score DESC LIMIT 5");

      assertThat(rs.hasNext()).isTrue();
      final Result plan = rs.next();
      final String planStr = plan.toJSON().toString();

      // Verify the planner uses FetchFromIndexedFunctionStep instead of a full type scan
      assertThat(planStr).contains("FetchFromIndexedFunctionStep");
      assertThat(planStr).doesNotContain("FetchFromTypeWithFilterStep");
    });
  }
}
