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
 * Regression test for issue #7790: {@code AlterTypeStatement} disagreed with itself about which of
 * {@code identifierValue} and {@code identifierListValue}/{@code identifierListAddRemove} a given ALTER carries.
 * {@code toString()} rendered {@code SUPERTYPE +X}/{@code BUCKET +x} as the literal text {@code null} (unparseable)
 * and dropped the trailing {@code WITH} clause, {@code copy()} silently dropped {@code identifierValue} (so a copied
 * {@code ALTER TYPE A NAME B} has no new name at all), and {@code getIdentityElements()} omitted both, so two ALTERs
 * that do different things compared {@code equals() == true}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7790AlterTypeStatementTest extends AbstractParserTest {

  @Test
  void toStringRoundTripsSuperTypeWithSigns() {
    checkRightSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, +Car");
    checkRightSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, -Car");
    checkRightSyntax("ALTER TYPE Suv SUPERTYPE Vehicle");
  }

  @Test
  void toStringRoundTripsBucketWithSigns() {
    // No comma between bucket ops per grammar: BUCKET ((PLUS | MINUS) identifier)+
    checkRightSyntax("ALTER TYPE Foo BUCKET +bucket1 -bucket2");
  }

  @Test
  void toStringRoundTripsAliases() {
    checkRightSyntax("ALTER TYPE Foo ALIASES alias1, alias2");
    checkRightSyntax("ALTER TYPE Foo ALIASES NULL");
  }

  @Test
  void toStringRoundTripsNameAndBucketSelectionStrategyWithSettings() {
    checkRightSyntax("ALTER TYPE Foo NAME Bar");
    checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin`");
    checkRightSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `round-robin` WITH repartition = true");
  }

  @Test
  void toStringActuallyRendersTheSuperTypeInstead() {
    final AlterTypeStatement stmt = (AlterTypeStatement) checkSyntax("ALTER TYPE Suv SUPERTYPE +Vehicle, +Car", true);
    final StringBuilder builder = new StringBuilder();
    stmt.toString(null, builder);
    assertThat(builder.toString()).doesNotContain("null").contains("+Vehicle").contains("+Car");
  }

  @Test
  void copyPreservesTheNewNameForAlterName() {
    final AlterTypeStatement original = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo NAME Bar", true);
    final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

    assertThat(copy.identifierValue).isNotNull();
    assertThat(copy.identifierValue.getStringValue()).isEqualTo("Bar");
  }

  @Test
  void copyPreservesTheBucketSelectionStrategyImplementation() {
    final AlterTypeStatement original = (AlterTypeStatement) checkSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `thread`", true);
    final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

    assertThat(copy.identifierValue.getStringValue()).isEqualTo(original.identifierValue.getStringValue());
  }

  @Test
  void copyPreservesTheWithSettings() {
    final AlterTypeStatement original =
        (AlterTypeStatement) checkSyntax("ALTER TYPE Foo BUCKETSELECTIONSTRATEGY `partitioned('x')` WITH repartition = true", true);
    final AlterTypeStatement copy = (AlterTypeStatement) original.copy();

    assertThat(copy.settings.keySet().stream().map(Identifier::getStringValue).toList()).containsExactly("repartition");
    assertThat(copy).isEqualTo(original);
  }

  @Test
  void equalsDistinguishesDifferentNewNames() {
    final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME B", true);
    final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME C", true);

    assertThat(a).isNotEqualTo(b);
    assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
  }

  @Test
  void equalsDistinguishesDifferentBucketSelectionStrategies() {
    final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `round-robin`", true);
    final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `thread`", true);

    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equalsDistinguishesDifferentWithSettings() {
    final AlterTypeStatement a =
        (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `thread` WITH repartition = true", true);
    final AlterTypeStatement b =
        (AlterTypeStatement) checkSyntax("ALTER TYPE A BUCKETSELECTIONSTRATEGY `thread` WITH repartition = false", true);

    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void equalsMatchesAnEquivalentStatement() {
    final AlterTypeStatement a = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME B", true);
    final AlterTypeStatement b = (AlterTypeStatement) checkSyntax("ALTER TYPE A NAME B", true);

    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }
}
