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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6997: {@link BinaryComparator#compare(Object, byte, Object, byte)} ordered two STRINGs by
 * UTF-16 code units ({@code String.compareTo}) while the LSM pages, {@code lookupInPage} and the sibling
 * {@link BinaryComparator#compareTo(Object, Object)} order them by unsigned UTF-8 bytes. The two orders disagree for any
 * pair of a non-BMP character (a surrogate pair, {@code 0xD800-0xDBFF} lead unit, {@code 0xF0-0xF4} lead byte) and a
 * BMP character above {@code U+E000}, so an indexed range scan whose stop bound sits on the wrong side of the disagreement
 * stopped before emitting anything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6997BinaryComparatorUtf8OrderTest extends TestHelper {

  /** U+FF21 FULLWIDTH LATIN CAPITAL LETTER A: one UTF-16 unit above every surrogate, three UTF-8 bytes. */
  private static final String FULLWIDTH_A = "Ａ";
  /** U+1F600 GRINNING FACE: a surrogate pair in UTF-16, four UTF-8 bytes. */
  private static final String EMOJI       = new String(Character.toChars(0x1F600));

  @Test
  void bothEntryPointsAgreeWithTheUtf8ByteOrder() {
    final BinaryComparator comparator = new BinaryComparator();

    // THE PREMISE: THE TWO ORDERS REALLY DISAGREE ON THIS PAIR
    assertThat(FULLWIDTH_A.compareTo(EMOJI)).isGreaterThan(0);
    assertThat(BinaryComparator.compareTo(FULLWIDTH_A, EMOJI)).isLessThan(0);

    assertThat(comparator.compare(FULLWIDTH_A, BinaryTypes.TYPE_STRING, EMOJI, BinaryTypes.TYPE_STRING))
        .as("compare() must order two STRINGs the way the pages do: unsigned UTF-8 bytes").isLessThan(0);
    assertThat(comparator.compare(EMOJI, BinaryTypes.TYPE_STRING, FULLWIDTH_A, BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare(EMOJI, BinaryTypes.TYPE_STRING, EMOJI, BinaryTypes.TYPE_STRING)).isZero();

    // THE ORDERS AGREE ON EVERYTHING ELSE, AND SO MUST THE FIX
    assertThat(comparator.compare("2", BinaryTypes.TYPE_STRING, "10", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare("abc", BinaryTypes.TYPE_STRING, "abd", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare("ab", BinaryTypes.TYPE_STRING, "abc", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare("abc", BinaryTypes.TYPE_STRING, "ab", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare("", BinaryTypes.TYPE_STRING, "", BinaryTypes.TYPE_STRING)).isZero();
    assertThat(comparator.compare("é", BinaryTypes.TYPE_STRING, "z", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare("", BinaryTypes.TYPE_STRING, "퟿", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    // A NON-STRING type2 STILL FALLS BACK TO ITS toString() FORM
    assertThat(comparator.compare("#1:0", BinaryTypes.TYPE_STRING, "#1:0", BinaryTypes.TYPE_COMPRESSED_RID)).isZero();
  }

  /**
   * An isolated surrogate has no code point, so the encoder writes a replacement byte for it and the UTF-16-as-UTF-8
   * remap no longer describes what the pages hold. Every shape of malformed input must still agree with the encoded
   * order, which is the order the pages use.
   */
  @Test
  void isolatedSurrogatesFollowTheEncodedOrder() {
    final String loneHigh = "\uD800";
    final String loneLow = "\uDC00";
    final String nonCharacter = "\uFFFE";
    final String[] samples = { loneHigh, loneLow, nonCharacter, EMOJI, FULLWIDTH_A, "a", "", "a" + loneHigh, loneHigh + "a",
        loneLow + "a", "a" + loneLow, EMOJI + "a", "a" + EMOJI, loneHigh + EMOJI, EMOJI + loneHigh, loneHigh + loneHigh,
        loneHigh + nonCharacter, "\uD800\uD800\uDC00", "\uD800\uDC00\uDC00" };

    for (final String a : samples)
      for (final String b : samples) {
        final int expected = Integer.signum(BinaryComparator.compareBytes(a.getBytes(StandardCharsets.UTF_8),
            b.getBytes(StandardCharsets.UTF_8)));
        assertThat(Integer.signum(BinaryComparator.compareStrings(a, b)))
            .as("'%s' vs '%s' must follow the unsigned UTF-8 order of the encodings", escape(a), escape(b)).isEqualTo(expected);
      }
  }

  private static String escape(final String s) {
    final StringBuilder out = new StringBuilder();
    for (final char c : s.toCharArray())
      out.append(c < 0x80 ? String.valueOf(c) : String.format("\\u%04X", (int) c));
    return out.toString();
  }

  @Test
  void indexedRangeScanOverABmpAndANonBmpKeyReturnsBothRows() {
    final DocumentType type = database.getSchema().createDocumentType("S");
    type.createProperty("k", Type.STRING);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");

    database.transaction(() -> {
      database.newDocument("S").set("k", FULLWIDTH_A).save();
      database.newDocument("S").set("k", EMOJI).save();
    });

    assertThat(keys("SELECT k FROM S", Map.of())).hasSize(2);

    assertThat(keys("SELECT k FROM S WHERE k <= :bound", Map.of("bound", EMOJI)))
        .as("both keys sort at or below the emoji in UTF-8 order").containsExactlyInAnyOrder(FULLWIDTH_A, EMOJI);
    assertThat(keys("SELECT k FROM S WHERE k >= :bound", Map.of("bound", FULLWIDTH_A)))
        .containsExactlyInAnyOrder(FULLWIDTH_A, EMOJI);
    assertThat(keys("SELECT k FROM S WHERE k < :bound", Map.of("bound", EMOJI))).containsExactly(FULLWIDTH_A);
    assertThat(keys("SELECT k FROM S WHERE k > :bound", Map.of("bound", FULLWIDTH_A))).containsExactly(EMOJI);
    assertThat(keys("SELECT k FROM S WHERE k BETWEEN :from AND :to", Map.of("from", FULLWIDTH_A, "to", EMOJI)))
        .containsExactlyInAnyOrder(FULLWIDTH_A, EMOJI);
    assertThat(keys("SELECT k FROM S WHERE k > :bound", Map.of("bound", EMOJI))).isEmpty();
  }

  private List<String> keys(final String query, final Map<String, Object> params) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext())
        keys.add(rs.next().getProperty("k"));
    }
    return keys;
  }
}
