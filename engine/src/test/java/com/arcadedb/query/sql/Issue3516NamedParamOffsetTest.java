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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3516: HTTP API params broken since 26.2.1
 * <p>
 * When using named parameters with keyword names like 'offset' in SKIP/LIMIT clauses,
 * the ANTLR parser was throwing a syntax error because 'offset' is a reserved keyword
 * but should be allowed as a parameter name.
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3516NamedParamOffsetTest extends TestHelper {

  @Test
  void testSkipWithOffsetParam() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Brewery IF NOT EXISTS");
      database.command("sql", "DELETE FROM Brewery");

      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Brewery SET name = 'Brewery" + i + "'");

      // Reproduce the exact query from the issue
      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 25);
      params.put("offset", 0);

      final ResultSet rs = database.query("sql", "select * from Brewery skip :offset limit :limit", params);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      rs.close();

      assertThat(results).hasSize(10);
    });
  }

  @Test
  void testSkipWithOffsetParamNonZero() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Brewery2 IF NOT EXISTS");
      database.command("sql", "DELETE FROM Brewery2");

      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Brewery2 SET name = 'Brewery" + i + "'");

      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 5);
      params.put("offset", 3);

      final ResultSet rs = database.query("sql", "select * from Brewery2 skip :offset limit :limit", params);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      rs.close();

      // skip 3, limit 5 => should return 5 results (10 - 3 = 7 available, capped at limit 5)
      assertThat(results).hasSize(5);
    });
  }

  @Test
  void testLimitOffsetOrder() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Brewery3 IF NOT EXISTS");
      database.command("sql", "DELETE FROM Brewery3");

      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Brewery3 SET name = 'Brewery" + i + "'");

      // Test with LIMIT before SKIP (alternative order)
      final Map<String, Object> params = new HashMap<>();
      params.put("limit", 4);
      params.put("offset", 2);

      final ResultSet rs = database.query("sql", "select * from Brewery3 limit :limit skip :offset", params);
      final List<Result> results = new ArrayList<>();
      while (rs.hasNext())
        results.add(rs.next());
      rs.close();

      // skip 2, limit 4 => should return 4 results
      assertThat(results).hasSize(4);
    });
  }
}
