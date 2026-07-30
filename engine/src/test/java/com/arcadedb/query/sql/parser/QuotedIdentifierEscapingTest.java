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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the escaping contract of back-tick quoted identifiers. A schema object name may contain a back-tick or a backslash, so the
 * escaping has to survive the parse -> re-emit -> re-parse cycle without altering the name and without letting the quoted token
 * absorb the SQL that follows it.
 */
class QuotedIdentifierEscapingTest {

  private static String reemit(final String sql) {
    final Statement statement = new SQLAntlrParser(null).parse(sql);
    final StringBuilder builder = new StringBuilder();
    statement.toString(null, builder);
    return builder.toString();
  }

  @Test
  void quotedInputIsNotEscapedTwice() {
    // `a\`b` is the SQL spelling of the name a`b - the inner text arrives already escaped and must not be escaped again
    final Identifier identifier = new Identifier(-1);
    identifier.setQuotedStringValue("`a\\`b`");

    assertThat(identifier.getStringValue()).isEqualTo("a`b");
    assertThat(identifier.getValue()).isEqualTo("a\\`b");
    assertThat(identifier.toString()).isEqualTo("`a\\`b`");
  }

  @Test
  void escapedBackTickNameSurvivesReEmission() {
    assertThat(reemit("SELECT FROM `a\\`b`")).isEqualTo("SELECT FROM `a\\`b`");
  }

  @Test
  void nameEndingWithBackslashSurvivesReEmission() {
    assertThat(reemit("SELECT FROM `T\\\\`")).isEqualTo("SELECT FROM `T\\\\`");
  }

  @Test
  void backslashTerminatedNameDoesNotAbsorbFollowingSql() {
    // the closing back-tick of `T\\` must terminate the identifier instead of pairing with the backslash, otherwise the
    // lexer runs on and swallows " SET `" into the type name
    assertThat(reemit("UPDATE `T\\\\` SET `v` = 1")).isEqualTo("UPDATE `T\\\\` SET `v` = 1");
  }

  @Test
  void backslashOnlyNameRoundTripsThroughIdentifier() {
    final Identifier identifier = new Identifier("T\\");

    assertThat(identifier.getStringValue()).isEqualTo("T\\");
  }

  @Test
  void backTickAndBackslashCombinedRoundTripsThroughIdentifier() {
    final Identifier identifier = new Identifier("a\\`b");

    assertThat(identifier.getStringValue()).isEqualTo("a\\`b");
  }

  /**
   * The trailing name part of a {@code schema:<kind>:<name>} target carries its own quoting, used whenever a bucket or index is
   * addressed by name, so it has to follow the same escaping rule as a plain quoted identifier.
   */
  @ParameterizedTest
  @ValueSource(strings = { "plain", "Back`Tick", "Trailing\\", "Inner\\Slash", "Mixed\\`Name", "Type[propA,propB]",
      "Evil` ; DROP TYPE Victim; --" })
  void schemaNamePartSurvivesQuoteUnquoteRoundTrip(final String namePart) {
    assertThat(SchemaIdentifier.unquoteName(SchemaIdentifier.quoteName(namePart))).isEqualTo(namePart);
  }

  @Test
  void schemaNamePartEndingWithBackslashDoesNotAbsorbFollowingSql() {
    assertThat(reemit("SELECT FROM schema:bucket:" + SchemaIdentifier.quoteName("Trailing\\") + " WHERE `v` = 1"))
        .isEqualTo("SELECT FROM schema:bucket:" + SchemaIdentifier.quoteName("Trailing\\") + " WHERE `v` = 1");
  }
}
