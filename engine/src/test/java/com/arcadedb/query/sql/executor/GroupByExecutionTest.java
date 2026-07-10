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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GroupByExecutionTest extends TestHelper {
  public GroupByExecutionTest() {
    autoStartTx = true;
  }

  @Test
  void groupByCount() {
    database.getSchema().createDocumentType("InputTx");

    for (int i = 0; i < 100; i++) {
      final String hash = UUID.randomUUID().toString();
      database.command("sql", "insert into InputTx set address = '" + hash + "'");

      // CREATE RANDOM NUMBER OF COPIES
      final int random = ThreadLocalRandom.current().nextInt(10);
      for (int j = 0; j < random; j++) {
        database.command("sql", "insert into InputTx set address = '" + hash + "'");
      }
    }

    final ResultSet result = database.query("sql", """
        select address, count(*) as occurrences
        from InputTx where address is not null
        group by address
        limit 10
        """);
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat(row.<String>getProperty("address")).isNotNull();
      assertThat(row.<Long>getProperty("occurrences")).isNotNull();
    }
    result.close();
  }

  // Regression test for issue #4855: GROUP BY was silently ignored on the second execution of the
  // same statement because the cached execution plan copy dropped the aggregation step.
  @Test
  void groupByRepeatedExecutionUsesCachedPlanCorrectly() {
    database.getSchema().createDocumentType("Tags");
    database.command("sql", "insert into Tags set tag = 'a'");
    database.command("sql", "insert into Tags set tag = 'a'");
    database.command("sql", "insert into Tags set tag = 'b'");

    for (int i = 0; i < 3; i++) {
      final ResultSet result = database.query("sql", "select tag from Tags group by tag");
      final Set<String> tags = new HashSet<>();
      int rowCount = 0;
      while (result.hasNext()) {
        tags.add(result.next().<String>getProperty("tag"));
        rowCount++;
      }
      result.close();

      assertThat(rowCount).as("iteration %d", i).isEqualTo(2);
      assertThat(tags).as("iteration %d", i).containsExactlyInAnyOrder("a", "b");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "select tag, count(*) as occurrences from Tags2 group by tag",
      "select tag, count(*) as occurrences from Tags2 group by tag;" })
  void groupBySqlScriptWithTrailingSemicolonIsNotIgnored(String sql) {
    database.getSchema().createDocumentType("Tags2");
    database.command("sql", "insert into Tags2 set tag = 'a'");
    database.command("sql", "insert into Tags2 set tag = 'a'");
    database.command("sql", "insert into Tags2 set tag = 'b'");

    final ResultSet result = database.command("sqlscript", sql);
    final Set<String> tags = new HashSet<>();
    final Map<String, Long> occurrencesByTag = new HashMap<>();
    int rowCount = 0;
    while (result.hasNext()) {
      final Result row = result.next();
      final String tag = row.getProperty("tag");
      tags.add(tag);
      occurrencesByTag.put(tag, row.getProperty("occurrences"));
      rowCount++;
    }
    result.close();

    assertThat(rowCount).isEqualTo(2);
    assertThat(tags).containsExactlyInAnyOrder("a", "b");
    assertThat(occurrencesByTag).containsEntry("a", 2L).containsEntry("b", 1L);
  }
}
