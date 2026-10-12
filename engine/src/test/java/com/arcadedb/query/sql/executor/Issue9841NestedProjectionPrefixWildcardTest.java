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
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9841: a prefix wildcard on the include side of a nested projection ({@code @this:{s*}}) was parsed, stored and
 * printed back, but the include loops evaluated it as the literal property {@code s}, answering a null column instead of
 * the properties the wildcard names. The exclude side ({@code !s*}) already honored it.
 */
class Issue9841NestedProjectionPrefixWildcardTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "create document type D1").close();
    database.transaction(() -> database.command("sql", "insert into D1 content {n:1, m:2, secret:'s', nested:{a:1, ab:2, b:3}}").close());
  }

  private Result project(final String projection) {
    try (final ResultSet rs = database.query("sql", "select " + projection + " as p from D1")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return row.getProperty("p");
    }
  }

  @Test
  void prefixWildcardOnDocumentSelectsMatchingProperties() {
    final Result p = project("@this:{s*}");
    assertThat(p.getPropertyNames()).containsExactly("secret");
    assertThat(p.<String>getProperty("secret")).isEqualTo("s");
  }

  @Test
  void prefixWildcardThatIsAlsoALiteralPropertyKeepsEveryMatch() {
    final Result p = project("@this:{n*, m}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("n", "nested", "m");
    assertThat(p.<Integer>getProperty("n")).isEqualTo(1);
    assertThat(p.<Integer>getProperty("m")).isEqualTo(2);
    assertThat(p.<Object>getProperty("nested")).isNotNull();
  }

  @Test
  void prefixWildcardMatchingNothingProducesNoColumn() {
    final Result p = project("@this:{zz*, m}");
    assertThat(p.getPropertyNames()).containsExactly("m");
  }

  @Test
  void prefixWildcardHonorsExcludeItemsOfTheSameBraces() {
    final Result p = project("@this:{n*, !nested}");
    assertThat(p.getPropertyNames()).containsExactly("n");
  }

  @Test
  void prefixWildcardOnMapValue() {
    final Result p = project("nested:{a*}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("a", "ab");
    assertThat(p.<Integer>getProperty("a")).isEqualTo(1);
    assertThat(p.<Integer>getProperty("ab")).isEqualTo(2);
  }

  @Test
  void prefixWildcardOnNonElementResult() {
    // A LET subquery yields projected rows: Result objects that are not records, so the nested projection takes its Result path
    try (final ResultSet rs = database.query("sql", "select $r:{s*} as p let $r = (select n, secret, m from D1)")) {
      final List<Result> rows = rs.next().getProperty("p");
      assertThat(rows).hasSize(1);
      final Result p = rows.getFirst();
      assertThat(p.isElement()).isFalse();
      assertThat(p.getPropertyNames()).containsExactly("secret");
      assertThat(p.<String>getProperty("secret")).isEqualTo("s");
    }
  }

  @Test
  void prefixWildcardAppliesNestedExpansionToEveryMatch() {
    final Result p = project("@this:{ne*:{a}}");
    assertThat(p.getPropertyNames()).containsExactly("nested");
    final Result nested = p.getProperty("nested");
    assertThat(nested.getPropertyNames()).containsExactly("a");
    assertThat(nested.<Integer>getProperty("a")).isEqualTo(1);
  }

  @Test
  void prefixWildcardOnMapValueHonorsExcludeItems() {
    final Result p = project("nested:{a*, !ab}");
    assertThat(p.getPropertyNames()).containsExactly("a");
  }

  @Test
  void overlappingItemsLetTheLastOneWin() {
    // n* emits nested as stored, then ne*:{a} names it again with an expansion: the later item's value is the one kept
    final Result p = project("@this:{n*, ne*:{a}}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("n", "nested");
    final Result nested = p.getProperty("nested");
    assertThat(nested.getPropertyNames()).containsExactly("a");
  }

  @Test
  void nonWildcardIncludeItemsKeepTheirBehaviour() {
    final Result p = project("@this:{n, nested.a as na}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("n", "na");
    assertThat(p.<Integer>getProperty("na")).isEqualTo(1);
  }

  @Test
  void aliasOnAPrefixWildcardIsRefused() {
    assertThatThrownBy(() -> project("@this:{se* as k}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("se*");
  }

  @Test
  void wildcardOnAnythingButAPlainPropertyNameIsRefused() {
    assertThatThrownBy(() -> project("@this:{nested.a*}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("nested.a*");
    assertThatThrownBy(() -> project("@this:{!nested.a*}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("nested.a*");
  }

  @Test
  void backtickQuotedPrefixIsAccepted() {
    final Result p = project("@this:{`se`*}");
    assertThat(p.getPropertyNames()).containsExactly("secret");
  }

  @Test
  void aliasOnAnExcludeWildcardIsIgnoredLikeOnAnyExcludeItem() {
    // An exclude item produces no column, so its alias has nothing to name: it is accepted and ignored, as on `!secret as k`
    final Result p = project("@this:{!se* as k}");
    assertThat(p.getPropertyNames()).doesNotContain("secret", "k").contains("n", "m", "nested");
  }

  @Test
  void prefixWildcardNextToTheBareStarAddsNothingTheStarDidNotSelect() {
    final Result p = project("@this:{*, s*}");
    assertThat(p.getPropertyNames()).contains("n", "m", "nested", "secret");
    assertThat(p.<String>getProperty("secret")).isEqualTo("s");
  }

  @Test
  void excludeItemRemovesAPropertyBothTheStarAndTheWildcardSelect() {
    final Result p = project("@this:{*, s*, !secret}");
    assertThat(p.getPropertyNames()).doesNotContain("secret").contains("n", "m", "nested");
  }

  @Test
  void prefixWildcardKeepsANullValuedProperty() {
    database.command("sql", "create document type D2").close();
    database.transaction(() -> database.command("sql", "insert into D2 content {sa:null, sb:1}").close());
    try (final ResultSet rs = database.query("sql", "select @this:{s*} as p from D2")) {
      final Result p = rs.next().getProperty("p");
      assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("sa", "sb");
      assertThat(p.<Object>getProperty("sa")).isNull();
    }
  }

  @Test
  void excludeWildcardStillFilters() {
    final Result p = project("@this:{!s*}");
    assertThat(p.getPropertyNames()).doesNotContain("secret").contains("n", "m", "nested");
  }
}
