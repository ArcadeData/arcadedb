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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #2776: expand() / UNWIND change last entry of variable from LET to last vertex.
 *
 * The bug: When using expand() or UNWIND with a LET clause that sets $nrid = @rid,
 * the last result row incorrectly shows a different RID instead of maintaining the original value.
 */
public class Issue2776ExpandUnwindLetTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // Create schema
      database.command("sql", "CREATE VERTEX TYPE News");
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE EDGE TYPE HasTag");

      // Insert News records
      for (int i = 1; i <= 10; i++) {
        database.command("sql", "INSERT INTO News CONTENT { \"id\": \"" + i + "\", \"title\": \"News " + i + "\", \"content\": \"Content " + i + "\" }");
      }

      // Insert Tag records
      for (int i = 1; i <= 10; i++) {
        database.command("sql", "INSERT INTO Tag CONTENT { \"id\": \"" + i + "\", \"name\": \"Tag " + i + "\" }");
      }

      // Create edges:
      // News 1 - Tags 1-5
      // News 2 - Tags 1-2
      // News 10 - Tags 6, 9, 10
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '1') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '2') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '3') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '4') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '5') TO (SELECT FROM News WHERE id = '1')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '1') TO (SELECT FROM News WHERE id = '2')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '2') TO (SELECT FROM News WHERE id = '2')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '6') TO (SELECT FROM News WHERE id = '10')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '9') TO (SELECT FROM News WHERE id = '10')");
      database.command("sql", "CREATE EDGE HasTag FROM (SELECT FROM Tag WHERE id = '10') TO (SELECT FROM News WHERE id = '10')");
    });
  }

  @Test
  public void testExpandWithLetVariable() {
    database.transaction(() -> {
      // First, find the RID of Tag with id = 1
      final ResultSet tagResult = database.query("sql", "SELECT @rid FROM Tag WHERE id = '1'");
      assertThat(tagResult.hasNext()).isTrue();
      final RID expectedRid = tagResult.next().getProperty("@rid");
      assertThat(expectedRid).isNotNull();

      // Query using expand() with LET clause
      // This should return 7 rows (5 edges to News 1 + 2 edges to News 2), all with $nrid = expectedRid
      final String query = "SELECT $nrid FROM (SELECT expand(out('HasTag').inE('HasTag')) FROM Tag LET $nrid = @rid WHERE id = '1')";
      final ResultSet result = database.query("sql", query);

      final List<RID> nridValues = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        final Object nridValue = row.getProperty("$nrid");
        assertThat(nridValue).isInstanceOf(RID.class);
        nridValues.add((RID) nridValue);
      }

      // We expect 7 results (edges from Tag 1 and Tag 2 to News 1 and News 2)
      assertThat(nridValues).hasSize(7);

      // All values should be the same RID (the RID of Tag with id = 1)
      for (int i = 0; i < nridValues.size(); i++) {
        assertThat(nridValues.get(i))
            .as("Row %d should have $nrid = %s but was %s", i, expectedRid, nridValues.get(i))
            .isEqualTo(expectedRid);
      }
    });
  }

  @Test
  public void testUnwindWithLetVariable() {
    database.transaction(() -> {
      // First, find the RID of Tag with id = 1
      final ResultSet tagResult = database.query("sql", "SELECT @rid FROM Tag WHERE id = '1'");
      assertThat(tagResult.hasNext()).isTrue();
      final RID expectedRid = tagResult.next().getProperty("@rid");
      assertThat(expectedRid).isNotNull();

      // Query using UNWIND with LET clause
      final String query = "SELECT $nrid FROM (SELECT out('HasTag').inE('HasTag') AS tags FROM Tag LET $nrid = @rid WHERE id = '1' UNWIND tags)";
      final ResultSet result = database.query("sql", query);

      final List<RID> nridValues = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        final Object nridValue = row.getProperty("$nrid");
        assertThat(nridValue).isInstanceOf(RID.class);
        nridValues.add((RID) nridValue);
      }

      // We expect 7 results
      assertThat(nridValues).hasSize(7);

      // All values should be the same RID (the RID of Tag with id = 1)
      for (int i = 0; i < nridValues.size(); i++) {
        assertThat(nridValues.get(i))
            .as("Row %d should have $nrid = %s but was %s", i, expectedRid, nridValues.get(i))
            .isEqualTo(expectedRid);
      }
    });
  }
}
