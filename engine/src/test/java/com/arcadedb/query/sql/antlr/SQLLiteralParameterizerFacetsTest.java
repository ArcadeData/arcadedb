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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.TestHelper;
import com.arcadedb.query.literal.LiteralParameterizer.Lookup;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The literal parameterizer (#8307) must leave every literal of a FACETS clause in the text: the planner sizes and buckets on
 * those values, so they are part of the statement, never parameters.
 */
class SQLLiteralParameterizerFacetsTest extends TestHelper {

  /** The first text of a shape decides its policy, the second one is answered from the parameterized statement. */
  private static Lookup<Statement> lookupTwice(final SQLLiteralParameterizer cache, final String text) {
    cache.lookup(text, true);
    return cache.lookup(text, true);
  }

  @Test
  void facetLiteralsAreKeptWhereLiteralsAreExtracted() {
    final SQLLiteralParameterizer cache = new SQLLiteralParameterizer(new SQLAntlrParser(database), 16);
    final Lookup<Statement> p = lookupTwice(cache,
        "SELECT FROM Asset WHERE size > 5 FACETS brand TERMS TOP 10 INCLUDE ['Nike'], s: size RANGES ([0, 100) AS 'a')");

    // the WHERE literal is extracted, every FACETS literal stays in the text
    assertThat(p.parameters()).isEqualTo(Map.of("__lit_i0", 5));
    assertThat(p.cacheKey()).contains("WHERE size > :__lit_i0").contains("TOP 10").contains("['Nike']").contains("[0, 100)")
        .contains("'a'");
    final SelectStatement statement = (SelectStatement) p.statement();
    assertThat(statement.getFacets().items.get(0).top.getValue()).isEqualTo(10);
    assertThat(statement.getFacets().items.get(1).ranges.get(0).to).isEqualTo(100);

    // a FILTER condition and a histogram keep their literals too
    final Lookup<Statement> h = lookupTwice(cache,
        "SELECT FROM Asset WHERE size > 7 FACETS b TERMS FILTER b = 'x', d: day HISTOGRAM FIXED '1h' MIN_COUNT 2 BOUNDS [1, 9]");
    assertThat(h.parameters()).isEqualTo(Map.of("__lit_i0", 7));
    assertThat(h.cacheKey()).contains("FILTER b = 'x'").contains("FIXED '1h'").contains("MIN_COUNT 2").contains("BOUNDS [1, 9]");
  }

  @Test
  void differentRangesDoNotShareAStatement() {
    final SQLLiteralParameterizer cache = new SQLLiteralParameterizer(new SQLAntlrParser(database), 16);
    final Lookup<Statement> first = lookupTwice(cache, "SELECT FROM Asset WHERE size > 5 FACETS s: size RANGES ([0, 100))");
    final Lookup<Statement> second = lookupTwice(cache, "SELECT FROM Asset WHERE size > 5 FACETS s: size RANGES ([0, 200))");

    assertThat(first.cacheKey()).isNotEqualTo(second.cacheKey());
    assertThat(first.statement()).isNotSameAs(second.statement());
    assertThat(((SelectStatement) first.statement()).getFacets().items.get(0).ranges.get(0).to).isEqualTo(100);
    assertThat(((SelectStatement) second.statement()).getFacets().items.get(0).ranges.get(0).to).isEqualTo(200);

    // while the WHERE literal alone still shares the statement of its shape
    final Lookup<Statement> third = lookupTwice(cache, "SELECT FROM Asset WHERE size > 6 FACETS s: size RANGES ([0, 100))");
    assertThat(third.statement()).isSameAs(first.statement());
    assertThat(third.parameters()).isEqualTo(Map.of("__lit_i0", 6));
  }
}
