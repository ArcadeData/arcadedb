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
import com.arcadedb.database.Binary;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7840: an LSM composite key whose components are long strings (the reported shape is a
 * 5-column UNIQUE index of four short scoping keys plus a project-relative file path) compared one code unit - and one
 * page byte - at a time on every probe, which made a seek cost proportional to the key width with no vectorisation.
 * <p>
 * {@link BinaryComparator#compareBytes(byte[], Binary)}, the probe's raw-bytes comparison of a key component against
 * the page, walked the run with one bounds-checked {@code getByte()} per byte; it now skips the common prefix in bulk.
 * {@link Binary#getString()} decoded through a throwaway {@code byte[]} that the {@code String} constructor copied
 * again; it now decodes straight out of the backing array.
 * <p>
 * What the tests below lock in is EQUIVALENCE: the buffer comparison must answer what the unsigned UTF-8 order
 * answers and must leave the read position exactly where the byte walk left it, because {@code lookupInPage} reads
 * the next component of a composite key from there; and both string readers must still return what was written, and
 * still refuse a corrupt length prefix. {@link BinaryComparator#compareStrings(String, String)} is unchanged by this
 * work - the cross-product below simply pins the ordering it has to keep answering.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7840WideKeyComparisonTest extends TestHelper {

  /** The reported index: four short scoping keys plus a long, high-cardinality trailing path. */
  private static final String[] KEY_COLUMNS = { "organizationId", "projectId", "branch", "indexAttemptId", "path" };

  @Test
  void compareStringsFollowsTheEncodedOrderOnEveryShape() {
    // EVERY REGION THE FAST PATH HAS TO GET RIGHT: ASCII, LATIN-1, THE BMP BELOW AND ABOVE THE SURROGATE BLOCK, A
    // WELL-FORMED PAIR, AND BOTH HALVES OF A PAIR ON THEIR OWN - THE ONE CASE THE REMAP ITSELF CANNOT HANDLE
    final String[] alphabet = { "a", "b", "Z", "0", "/", "é", "\u07FF", "\u0800", "\uD7FF", "\uE000", "\uFFFD", "\uFFFE",
        "\uFF21", new String(Character.toChars(0x1F600)), new String(Character.toChars(0x10000)), "\uD800", "\uDBFF", "\uDC00",
        "\uDFFF" };

    final List<String> samples = new ArrayList<>();
    samples.add("");
    for (final String first : alphabet) {
      samples.add(first);
      for (final String second : alphabet)
        samples.add(first + second);
    }
    // AND THE SHAPE THE ISSUE IS ABOUT: A LONG SHARED PREFIX WITH THE DIFFERENCE AT THE VERY END
    final String prefix = "src/main/java/com/acme/platform/service/internal/";
    for (final String tail : alphabet)
      samples.add(prefix + tail);

    for (final String a : samples)
      for (final String b : samples)
        assertThat(Integer.signum(BinaryComparator.compareStrings(a, b)))
            .as("'%s' vs '%s' must follow the unsigned UTF-8 order of the encodings", escape(a), escape(b))
            .isEqualTo(Integer.signum(BinaryComparator.compareBytes(a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  void compareBytesAgainstABufferAnswersAndAdvancesLikeTheByteWalk() {
    final byte[][] runs = { new byte[0], bytes(0x00), bytes(0x41), bytes(0x41, 0x42), bytes(0x41, 0x42, 0x43),
        bytes(0x41, 0x42, 0x44), bytes(0x41, 0x42, 0x43, 0x44), bytes(0x7F), bytes(0x80), bytes(0xFF),
        // THE #5321 SHAPE: A CONTINUATION BYTE IS NEGATIVE AS A JAVA byte AND MUST STILL SORT ABOVE ASCII
        "é".getBytes(StandardCharsets.UTF_8), "src/main/java/com/acme/platform/Handler1.java".getBytes(StandardCharsets.UTF_8),
        "src/main/java/com/acme/platform/Handler2.java".getBytes(StandardCharsets.UTF_8),
        "src/main/java/com/acme/platform/Handler1.javax".getBytes(StandardCharsets.UTF_8) };

    final BinaryComparator comparator = new BinaryComparator();
    for (final byte[] key : runs)
      for (final byte[] stored : runs) {
        final Binary page = pageWith(stored);
        final int start = page.position();

        final int result = comparator.compareBytes(key, page);

        assertThat(Integer.signum(result)).as("%s vs %s", Arrays.toString(key), Arrays.toString(stored))
            .isEqualTo(Integer.signum(BinaryComparator.compareBytes(key, stored)));
        assertThat(page.position() - start - varIntSize(stored.length))
            .as("the position must land where one getByte() per compared byte would have left it")
            .isEqualTo(expectedConsumedBytes(key, stored));
      }
  }

  @Test
  void compareBytesReadsThroughASliceWithANonZeroArrayOffset() {
    // A PAGE IS ALWAYS HANDED OVER AS A SLICE, SO THE BACKING ARRAY OFFSET IS NEVER 0: A BULK READ THAT IGNORED IT
    // WOULD COMPARE THE WRONG BYTES ENTIRELY
    final byte[] stored = "src/main/java/com/acme/platform/Handler1.java".getBytes(StandardCharsets.UTF_8);

    final Binary whole = new Binary();
    whole.putByteArray(new byte[] { 9, 9, 9, 9, 9, 9, 9 }); // A HEADER THE SLICE STARTS AFTER
    final int contentAt = whole.position();
    whole.putBytes(stored);
    whole.flip();

    final Binary slice = whole.slice(contentAt);
    final BinaryComparator comparator = new BinaryComparator();

    assertThat(comparator.compareBytes(stored, slice)).isZero();
    assertThat(slice.position()).isEqualTo(varIntSize(stored.length) + stored.length);
  }

  @Test
  void mismatchAdvancesLikeTheByteWalkAndRefusesARunLongerThanTheBufferHolds() {
    final Binary page = new Binary();
    page.putByteArray(bytes(0x41, 0x42, 0x43, 0x44));
    page.flip();

    assertThat(page.mismatch(bytes(0x41, 0x42), 2)).as("an identical run has no mismatch").isEqualTo(-1);
    assertThat(page.position()).as("and leaves the position past the whole run, like two getByte() calls").isEqualTo(2);

    page.position(0);
    assertThat(page.mismatch(bytes(0x41, 0x99), 2)).as("the offset of the first difference").isEqualTo(1);
    assertThat(page.position()).as("and the position past the differing byte, like two getByte() calls").isEqualTo(2);

    page.position(0);
    assertThatThrownBy(() -> page.mismatch(bytes(0x41, 0x42, 0x43, 0x44, 0x45), 5))
        .as("a run longer than the buffer holds fails like the getByte() walk it replaces")
        .isInstanceOf(BufferUnderflowException.class);
    assertThat(page.position()).as("and leaves the position untouched").isZero();
  }

  @Test
  void aStringReadsBackByteForByteWhateverWasWritten() {
    // getString() no longer routes through getBytes(), so the two readers have to be shown to still agree - on an
    // empty string, on multi-byte UTF-8, on a surrogate pair, and through a slice whose backing array offset is not 0
    final String[] samples = { "", "a", "src/main/java/com/acme/platform/Handler1.java", "\u00e9\u00e0\u00fc",
        "\u4e2d\u6587", new String(Character.toChars(0x1F600)), "mixed \u00e9 " + new String(Character.toChars(0x1F600)) };

    for (final String sample : samples) {
      final Binary whole = new Binary();
      whole.putByteArray(new byte[] { 7, 7, 7 }); // A HEADER, SO THE SLICE BELOW CARRIES A NON-ZERO ARRAY OFFSET
      final int contentAt = whole.position();
      whole.putString(sample);
      final int afterContent = whole.position();
      whole.flip();

      whole.position(contentAt);
      assertThat(whole.getString()).as("read back in place").isEqualTo(sample);
      assertThat(whole.position()).as("the position must land past the content").isEqualTo(afterContent);

      assertThat(whole.getString(contentAt)).as("read back by absolute index").isEqualTo(sample);

      whole.position(contentAt);
      assertThat(new String(whole.getBytes(), StandardCharsets.UTF_8)).as("getBytes() must still agree").isEqualTo(sample);

      final Binary slice = whole.slice(contentAt);
      assertThat(slice.getString()).as("read back through a slice").isEqualTo(sample);
    }
  }

  @Test
  void aCorruptLengthPrefixIsStillRefusedByBothReaders() {
    // readLengthPrefix() is now shared, so the #4420 guard has to hold on the string path too and not decode whatever
    // happens to sit past the end of the buffer
    final Binary page = new Binary();
    page.putUnsignedNumber(64); // CLAIMS 64 BYTES
    page.putByteArray(bytes(0x41, 0x42));
    page.flip();

    assertThatThrownBy(page::getString).isInstanceOf(SerializationException.class);
    page.position(0);
    assertThatThrownBy(page::getBytes).isInstanceOf(SerializationException.class);
  }

  @Test
  void aSeekOnAWideCompositeKeyFindsExactlyItsOwnRow() {
    // THE REPORTED WORKLOAD: ONE UNIQUE 5-COLUMN INDEX WHOSE FOUR LEADING COMPONENTS REPEAT ACROSS EVERY ROW, SO THE
    // WHOLE DECISION FALLS ON THE LONG TRAILING PATH - EXACTLY THE COMPARISON BOTH OPTIMISATIONS TOUCH
    final int rows = 2_000;
    final DocumentType file = database.getSchema().createVertexType("File");
    for (final String column : KEY_COLUMNS)
      file.createProperty(column, Type.STRING);
    file.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, KEY_COLUMNS);
    database.getSchema().createEdgeType("IMPORTS");

    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        database.newVertex("File").set("organizationId", "org-0001").set("projectId", "proj-0001").set("branch", "main")
            .set("indexAttemptId", "attempt-0001").set("path", pathOf(i)).save();
    });

    database.transaction(() -> {
      for (int i = 0; i < rows; i++) {
        final List<String> found = pathsOf(i);
        assertThat(found).as("the seek for path %d must answer its own row and nothing else", i).containsExactly(pathOf(i));
      }
    });

    // AND THE EDGE HALF OF THE REPORTED BLOCK, WHICH SEEKS BOTH ENDPOINTS THROUGH THE SAME INDEX
    database.transaction(() -> database.command("sqlscript", """
        LET a = SELECT FROM File WHERE organizationId = 'org-0001' AND projectId = 'proj-0001' AND branch = 'main'
                AND indexAttemptId = 'attempt-0001' AND path = :src;
        LET b = SELECT FROM File WHERE organizationId = 'org-0001' AND projectId = 'proj-0001' AND branch = 'main'
                AND indexAttemptId = 'attempt-0001' AND path = :tgt;
        IF ($a.size() > 0 AND $b.size() > 0) {
          CREATE EDGE IMPORTS FROM $a TO $b SET line = 12;
        }
        """, Map.of("src", pathOf(7), "tgt", pathOf(1_234))).close());

    try (final ResultSet rs = database.query("sql", "SELECT out().path AS target FROM File WHERE path = :p",
        Map.of("p", pathOf(7)))) {
      assertThat(rs.next().<List<String>>getProperty("target")).containsExactly(pathOf(1_234));
    }
  }

  private List<String> pathsOf(final int i) {
    final List<String> found = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", """
        SELECT path FROM File WHERE organizationId = 'org-0001' AND projectId = 'proj-0001' AND branch = 'main'
          AND indexAttemptId = 'attempt-0001' AND path = :p""", Map.of("p", pathOf(i)))) {
      while (rs.hasNext())
        found.add(rs.next().getProperty("path"));
    }
    return found;
  }

  /** A project-relative path: a long prefix shared with thousands of siblings and a difference only near the end. */
  private static String pathOf(final int i) {
    return "src/main/java/com/acme/platform/service/internal/generated/Handler" + i + "ServiceImplementation.java";
  }

  /** The bytes a {@code lookupInPage} probe walks: a var-int length prefix followed by the stored key component. */
  private static Binary pageWith(final byte[] stored) {
    final Binary page = new Binary();
    page.putBytes(stored);
    page.flip();
    return page;
  }

  /** How far one {@code getByte()} per compared byte leaves the position past the length prefix. */
  private static int expectedConsumedBytes(final byte[] key, final byte[] stored) {
    final int min = Math.min(key.length, stored.length);
    for (int i = 0; i < min; i++)
      if (key[i] != stored[i])
        return i + 1;
    return min;
  }

  private static int varIntSize(final int value) {
    final Binary sizer = new Binary();
    return sizer.putUnsignedNumber(value);
  }

  private static byte[] bytes(final int... values) {
    final byte[] result = new byte[values.length];
    for (int i = 0; i < values.length; i++)
      result[i] = (byte) values[i];
    return result;
  }

  private static String escape(final String s) {
    final StringBuilder out = new StringBuilder();
    for (final char c : s.toCharArray())
      out.append(c >= 0x20 && c < 0x7F ? String.valueOf(c) : String.format("\\u%04X", (int) c));
    return out.toString();
  }
}
