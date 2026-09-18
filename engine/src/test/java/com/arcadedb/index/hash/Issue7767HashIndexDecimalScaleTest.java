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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7767: the #7613 decimal-scale fix reached every comparator-based caller, and the HASH
 * index is not one. It serializes the key, hashes those bytes to route it and compares them RAW to settle equality,
 * and a {@link BigDecimal} serializes its scale - so {@code 5} and {@code 5.00} produced different bytes, a
 * different slot and a non-matching entry. A UNIQUE_HASH index on a DECIMAL property therefore did not enforce its
 * constraint across scales, and a lookup for one spelling of a number could not find a row stored under another.
 * <p>
 * Fixed by canonicalizing in {@code HashIndex.convertKeys()} - the single funnel every put/get/remove passes through
 * - with the rule that used to live privately inside the LSM index's bloom-filter guard and is now shared as
 * {@link BinaryComparator#canonicalizeForByteEquality}.
 * <p>
 * The LSM twin of each assertion is run alongside, as the control: it passed before this fix and must keep passing,
 * which is what identifies the defect as the site #7613 did not reach rather than a general property of DECIMAL
 * keys.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7767HashIndexDecimalScaleTest extends TestHelper {

  @Test
  void aUniqueHashIndexOnADecimalRefusesTheSameNumberSpelledWithAnotherScale() {
    assertScaleIsOneKey(Schema.INDEX_TYPE.HASH, "HashPrices");
  }

  @Test
  void theLsmTwinBehavesTheSameWay() {
    assertScaleIsOneKey(Schema.INDEX_TYPE.LSM_TREE, "LsmPrices");
  }

  @Test
  void canonicalizationIsSharedAndLeavesEverythingElseUntouched() {
    final Object five = new BigDecimal("5");
    assertThat(BinaryComparator.canonicalizeForByteEquality(new BigDecimal("5.00"))).isEqualTo(five);
    // Already canonical: the very same instance comes back, so the common path allocates nothing.
    assertThat(BinaryComparator.canonicalizeForByteEquality(five)).isSameAs(five);
    assertThat(BinaryComparator.canonicalizeForByteEquality("5.00")).isEqualTo("5.00");
    assertThat(BinaryComparator.canonicalizeForByteEquality((Object) null)).isNull();

    final Object[] untouched = { "a", 1L };
    assertThat(BinaryComparator.canonicalizeForByteEquality(untouched)).isSameAs(untouched);

    final Object[] composite = { "a", new BigDecimal("1.10") };
    final Object[] canonical = BinaryComparator.canonicalizeForByteEquality(composite);
    assertThat(canonical).isNotSameAs(composite);
    assertThat(canonical[1]).isEqualTo(new BigDecimal("1.1"));
    assertThat(composite[1]).as("the caller's array must not be rewritten").isEqualTo(new BigDecimal("1.10"));
  }

  private void assertScaleIsOneKey(final Schema.INDEX_TYPE indexType, final String typeName) {
    final TypeIndex[] holder = new TypeIndex[1];
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(typeName);
      type.createProperty("amount", Type.DECIMAL);
      holder[0] = type.createTypeIndex(indexType, true, "amount");

      database.newDocument(typeName).set("amount", new BigDecimal("5")).save();
    });
    final TypeIndex index = holder[0];

    database.transaction(() -> {
      try (final IndexCursor byTheStoredSpelling = index.get(new Object[] { new BigDecimal("5") })) {
        assertThat(byTheStoredSpelling.hasNext()).isTrue();
      }
      try (final IndexCursor byAnotherScale = index.get(new Object[] { new BigDecimal("5.00") })) {
        assertThat(byAnotherScale.hasNext())
            .as("%s: 5.00 and 5 are one key to the comparator, so they must be one key to the index", indexType)
            .isTrue();
      }
    });

    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument(typeName).set("amount", new BigDecimal("5.00")).save()))
        .as("%s: a UNIQUE index must not admit the same number twice because it was spelled differently", indexType)
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType(typeName, false)).isEqualTo(1);
  }
}
