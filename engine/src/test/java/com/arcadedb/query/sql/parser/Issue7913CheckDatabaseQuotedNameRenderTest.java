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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7913: the TYPE and BUCKET loops in {@code CheckDatabaseStatement.toString()} appended the
 * raw name ({@code Identifier.getStringValue()} / {@code BucketIdentifier.getValue()}) instead of letting the
 * element render itself, as the RECORD loop three lines below already did. A back-tick quoted name lost its
 * quotes, so the render no longer reparsed - and the statement cache re-parses the rendered text, so a statement
 * whose render does not reparse cannot round-trip. #7793 fixed the separator on these same two loops; the element
 * rendering next to it was left untouched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7913CheckDatabaseQuotedNameRenderTest extends AbstractParserTest {

  @Test
  void aQuotedTypeNameWithASpaceKeepsItsBackTicks() {
    final Statement parsed = (Statement) checkRightSyntax("CHECK DATABASE TYPE `My Type`");

    assertThat(parsed.toString()).isEqualTo("CHECK DATABASE TYPE `My Type`");
    // And the render reparses, which is the point: the statement cache re-parses its own output.
    assertThat(checkRightSyntax(parsed.toString()).toString()).isEqualTo(parsed.toString());
  }

  @Test
  void aQuotedBucketNameWithASpaceKeepsItsBackTicks() {
    final Statement parsed = (Statement) checkRightSyntax("CHECK DATABASE BUCKET `my bucket`");

    assertThat(parsed.toString()).isEqualTo("CHECK DATABASE BUCKET `my bucket`");
    assertThat(checkRightSyntax(parsed.toString()).toString()).isEqualTo(parsed.toString());
  }

  /**
   * A quoted RESERVED WORD: this one reparsed even before the fix, but as a different statement - the quotes are
   * what say "a type called Order", not the ORDER keyword - so the round-trip silently stopped comparing equal.
   */
  @Test
  void aQuotedReservedWordKeepsItsBackTicksAndReparsesEqual() {
    final CheckDatabaseStatement parsed = (CheckDatabaseStatement) new SQLAntlrParser(null)
        .parse("CHECK DATABASE TYPE `Order`");

    assertThat(parsed.toString()).isEqualTo("CHECK DATABASE TYPE `Order`");

    final CheckDatabaseStatement reparsed = (CheckDatabaseStatement) new SQLAntlrParser(null).parse(parsed.toString());
    assertThat(reparsed).isEqualTo(parsed);
  }

  /**
   * Mixed list: the quoted element keeps its quotes and the plain one does not grow any, with #7793's separator
   * still in place.
   */
  @Test
  void aMixedTypeListRendersEachElementInItsOwnForm() {
    final CheckDatabaseStatement parsed = (CheckDatabaseStatement) new SQLAntlrParser(null)
        .parse("CHECK DATABASE TYPE `My Type`, Customer");
    final String rendered = parsed.toString();

    assertThat(rendered).startsWith("CHECK DATABASE TYPE ");
    assertThat(rendered).contains("`My Type`").contains("Customer");
    assertThat(rendered).doesNotContain("``");
    assertThat(((CheckDatabaseStatement) new SQLAntlrParser(null).parse(rendered))).isEqualTo(parsed);
  }

  /**
   * A numeric bucket reference has no name to quote and must keep rendering as the bare number: the else arm now
   * splits on {@code bucketId} instead of falling through {@code getValue()}.
   */
  @Test
  void aNumericBucketStillRendersAsTheBareNumber() {
    final Statement parsed = (Statement) checkRightSyntax("CHECK DATABASE BUCKET 1, 2");

    assertThat(parsed.toString()).matches("CHECK DATABASE BUCKET \\d+,\\d+");
  }

  /**
   * {@code CREATE TYPE ... BUCKET} renders through the same {@code BucketIdentifier.toString()}, so it gets the
   * fix too.
   */
  @Test
  void createTypeRendersAQuotedBucketNameWithItsBackTicks() {
    final Statement parsed = (Statement) checkRightSyntax("CREATE DOCUMENT TYPE Foo BUCKET `my bucket`");

    assertThat(parsed.toString()).contains("`my bucket`");
    assertThat(checkRightSyntax(parsed.toString()).toString()).isEqualTo(parsed.toString());
  }
}
