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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parser-side regression test for issue #7920: the grammar accepts a comma-separated list of {@code alterTypeItem},
 * but the AST held ONE {@code property}, ONE {@code identifierValue} and one shared identifier list, which the
 * builder overwrote per item. So a multi-item {@code ALTER TYPE} kept only the last item, and because every item
 * appended into the same list the list could be carried from one item's property to another's -
 * {@code ALTER TYPE Foo ALIASES x, y, SUPERTYPE +A} re-rendered as {@code ALTER TYPE Foo supertype +x, +y, +A},
 * three super types out of two aliases and one super type.
 * <p>
 * {@code toString()} also emitted no comma before the {@code CUSTOM} arm, so the one multi-item form that DID
 * execute both items re-rendered as unparseable SQL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7920MultiItemAlterTypeTest extends AbstractParserTest {

  @Test
  void everyItemIsKept() {
    final AlterTypeStatement stmt = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo2 NAME Bar2, SUPERTYPE +Base", true);

    assertThat(stmt.items).hasSize(2);
    assertThat(stmt.items.get(0).property).isEqualTo("name");
    assertThat(stmt.items.get(0).identifierValue.getStringValue()).isEqualTo("Bar2");
    assertThat(stmt.items.get(1).property).isEqualTo("supertype");
    assertThat(stmt.items.get(1).identifierListValue).hasSize(1);
    assertThat(stmt.items.get(1).identifierListValue.get(0).getStringValue()).isEqualTo("Base");
  }

  @Test
  void aliasesDoNotLeakIntoTheFollowingSuperType() {
    final AlterTypeStatement stmt = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo ALIASES x, y, SUPERTYPE +A", true);

    assertThat(stmt.items).hasSize(2);
    assertThat(stmt.items.get(0).property).isEqualTo("aliases");
    assertThat(stmt.items.get(0).identifierListValue.stream().map(Identifier::getStringValue).toList())
        .containsExactly("x", "y");
    assertThat(stmt.items.get(1).property).isEqualTo("supertype");
    assertThat(stmt.items.get(1).identifierListValue.stream().map(Identifier::getStringValue).toList())
        .containsExactly("A");

    final StringBuilder builder = new StringBuilder();
    stmt.toString(null, builder);
    assertThat(builder.toString()).isEqualTo("ALTER TYPE Foo aliases x, y, supertype +A");
  }

  @Test
  void multiItemFormsRoundTrip() {
    // checkRightSyntax re-parses what toString() rendered, so each of these is a round trip.
    checkRightSyntax("ALTER TYPE Foo NAME Bar, SUPERTYPE +Base");
    checkRightSyntax("ALTER TYPE Foo NAME Bar, CUSTOM description = 'x'");
    checkRightSyntax("ALTER TYPE Foo ALIASES x, y, SUPERTYPE +A");
    checkRightSyntax("ALTER TYPE Foo SUPERTYPE +A, -B, BUCKET +b1 -b2, CUSTOM k = 1");
  }

  @Test
  void customIsRenderedWithItsSeparatingComma() {
    final AlterTypeStatement stmt =
        (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar, CUSTOM description = 'x'", true);

    final StringBuilder builder = new StringBuilder();
    stmt.toString(null, builder);
    // Without the comma this rendered `ALTER TYPE Foo name Bar CUSTOM description='x'`, which does not parse.
    assertThat(builder.toString()).isEqualTo("ALTER TYPE Foo name Bar, CUSTOM description='x'");
  }

  @Test
  void copyCarriesEveryItem() {
    final AlterTypeStatement original =
        (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar, SUPERTYPE +Base, CUSTOM k = 'v'", true);
    final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

    assertThat(copy.items).hasSize(3);
    assertThat(copy).isEqualTo(original);
    assertThat(copy.hashCode()).isEqualTo(original.hashCode());
  }

  @Test
  void equalsDistinguishesStatementsThatDifferOnlyInADroppedItem() {
    final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar, SUPERTYPE +Base", true);
    final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Baz, SUPERTYPE +Base", true);

    // Both used to collapse onto the same trailing SUPERTYPE item, so they compared equal.
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void singleItemFormsAreUnaffected() {
    checkRightSyntax("ALTER TYPE Foo NAME Bar");
    checkRightSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, +Car");
    checkRightSyntax("ALTER TYPE Foo BUCKET +bucket1 -bucket2");
    checkRightSyntax("ALTER TYPE Foo ALIASES alias1, alias2");
    checkRightSyntax("ALTER TYPE Foo ALIASES NULL");
    checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin`");
    checkRightSyntax("ALTER TYPE Foo CUSTOM description = 'x'");
    checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin` WITH repartition = true");
  }
}
