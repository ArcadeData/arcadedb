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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9124: {@code WHERE r:R} compared relationship type names for equality while the pattern {@code [r:R]} is
 * polymorphic, so a relationship of a type that extends {@code R} was returned by the pattern and not by the predicate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9124RelationshipSubtypeLabelCheckTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE N9124");
      database.command("sql", "CREATE EDGE TYPE R9124");
      database.command("sql", "CREATE EDGE TYPE R9124Sub EXTENDS R9124");
      database.command("sql", "CREATE EDGE TYPE Other9124");
      database.command("sql", "CREATE VERTEX N9124 SET id = 1");
      database.command("sql", "CREATE VERTEX N9124 SET id = 2");
      for (final String edge : new String[] { "R9124", "R9124Sub", "Other9124" })
        database.command("sql", "CREATE EDGE " + edge + " FROM (SELECT FROM N9124 WHERE id = 1) TO (SELECT FROM N9124 WHERE id = 2)");
    });
  }

  private List<String> types(final String query) {
    final List<String> types = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        types.add(rs.next().getProperty("t"));
    }
    return types.stream().sorted().toList();
  }

  @Test
  void patternAndPredicateAgreeOnSubtypes() {
    final List<String> pattern = types("MATCH ()-[r:R9124]->() RETURN type(r) AS t");
    assertThat(pattern).containsExactly("R9124", "R9124Sub");
    assertThat(types("MATCH ()-[r]->() WHERE r:R9124 RETURN type(r) AS t")).isEqualTo(pattern);
  }

  @Test
  void disjunctionAgreesWithThePatternToo() {
    assertThat(types("MATCH ()-[r:R9124|Other9124]->() RETURN type(r) AS t")).containsExactly("Other9124", "R9124", "R9124Sub");
    assertThat(types("MATCH ()-[r]->() WHERE r:R9124|Other9124 RETURN type(r) AS t")).containsExactly("Other9124", "R9124", "R9124Sub");
  }

  @Test
  void aTwoLevelSubtypeChainMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE EDGE TYPE R9124Leaf EXTENDS R9124Sub");
      database.command("sql", "CREATE EDGE R9124Leaf FROM (SELECT FROM N9124 WHERE id = 1) TO (SELECT FROM N9124 WHERE id = 2)");
    });
    assertThat(types("MATCH ()-[r:R9124]->() RETURN type(r) AS t")).containsExactly("R9124", "R9124Leaf", "R9124Sub");
    assertThat(types("MATCH ()-[r]->() WHERE r:R9124 RETURN type(r) AS t")).containsExactly("R9124", "R9124Leaf", "R9124Sub");
    assertThat(types("MATCH ()-[r]->() WHERE r:R9124Sub RETURN type(r) AS t")).containsExactly("R9124Leaf", "R9124Sub");
  }

  @Test
  void aSiblingTypeIsStillRefused() {
    assertThat(types("MATCH ()-[r]->() WHERE r:R9124Sub RETURN type(r) AS t")).containsExactly("R9124Sub");
  }
}
