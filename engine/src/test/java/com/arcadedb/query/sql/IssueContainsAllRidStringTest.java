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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the Discord-reported bug: CONTAINSALL fails when comparing a
 * collection of Identifiables (such as the result of {@code outE('...').@in}) with a
 * literal collection of RID strings.
 * <p>
 * Reproduces the user's pattern:
 * <pre>
 *   SELECT FROM (SELECT *, outE('HasBrewery').@in AS brewery FROM Beer WHERE name='Hocus Pocus')
 *   WHERE brewery CONTAINSALL ["#X:Y"]
 * </pre>
 * The inner query yields {@code brewery} as a list of vertices; the outer CONTAINSALL
 * compares that list against a list of RID strings. Type-aware equality (already used by
 * CONTAINS / CONTAINSANY) must be applied so the string {@code "#X:Y"} matches the
 * Identifiable with the same RID.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class IssueContainsAllRidStringTest extends TestHelper {

  @Test
  void containsAllMatchesRidStringsAgainstIdentifiables() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Beer IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Brewery IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE HasBrewery IF NOT EXISTS");

      final RID breweryRid = database.command("sql", "CREATE VERTEX Brewery SET name = 'Magic Hat'")
          .next().getIdentity().orElseThrow();

      database.command("sql", "CREATE VERTEX Beer SET name = 'Hocus Pocus'");
      database.command("sql",
          "CREATE EDGE HasBrewery FROM (SELECT FROM Beer WHERE name = 'Hocus Pocus') TO " + breweryRid);

      final String breweryRidStr = breweryRid.toString();

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT *, outE('HasBrewery').@in AS brewery FROM Beer WHERE name='Hocus Pocus') "
              + "WHERE brewery CONTAINSALL [\"" + breweryRidStr + "\"]")) {
        assertThat(rs.hasNext())
            .as("CONTAINSALL with a list of RID strings should match the inline 'brewery' list of vertices")
            .isTrue();
      }

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT *, outE('HasBrewery').@in AS brewery FROM Beer WHERE name='Hocus Pocus') "
              + "WHERE brewery CONTAINSALL " + breweryRidStr)) {
        assertThat(rs.hasNext())
            .as("CONTAINSALL with a single RID literal should match the inline 'brewery' list of vertices")
            .isTrue();
      }
    });
  }

  @Test
  void containsAllReturnsFalseWhenRidStringIsMissing() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Beer IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Brewery IF NOT EXISTS");
      database.command("sql", "CREATE EDGE TYPE HasBrewery IF NOT EXISTS");

      final RID linkedBreweryRid = database.command("sql", "CREATE VERTEX Brewery SET name = 'LinkedOne'")
          .next().getIdentity().orElseThrow();
      final RID otherBreweryRid = database.command("sql", "CREATE VERTEX Brewery SET name = 'OtherOne'")
          .next().getIdentity().orElseThrow();

      database.command("sql", "CREATE VERTEX Beer SET name = 'PlainBeer'");
      database.command("sql",
          "CREATE EDGE HasBrewery FROM (SELECT FROM Beer WHERE name = 'PlainBeer') TO " + linkedBreweryRid);

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM (SELECT *, outE('HasBrewery').@in AS brewery FROM Beer WHERE name='PlainBeer') "
              + "WHERE brewery CONTAINSALL [\"" + linkedBreweryRid + "\", \"" + otherBreweryRid + "\"]")) {
        assertThat(rs.hasNext())
            .as("CONTAINSALL must require every right-hand RID to be present in the brewery list")
            .isFalse();
      }
    });
  }
}
