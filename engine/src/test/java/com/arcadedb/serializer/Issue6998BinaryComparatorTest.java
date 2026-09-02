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
import com.arcadedb.query.sql.executor.QueryOperatorEquals;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6998, two independent defects in {@link BinaryComparator}.
 * <p>
 * 1. {@code equalsBytes} applied a "compare the last byte first" fast path with no guard for an empty array, so two
 * empty {@code byte[]} instances read index -1 and threw {@code ArrayIndexOutOfBoundsException} instead of comparing
 * equal. It is reachable from SQL: an empty BINARY property compared against a different empty {@code byte[]}
 * instance failed the query rather than matching it.
 * <p>
 * 2. {@code compareTo} encoded its left String operand with the JVM default charset and its right one with the
 * configured charset. On a non-UTF-8 default the two disagree, so an equal pair of non-ASCII strings compared as
 * less-than and ORDER BY / range predicates answered wrongly. That precondition cannot be created inside a running
 * JVM - {@code Charset.defaultCharset()} is fixed at startup - so the case is exercised in a forked JVM started with
 * {@code -Dfile.encoding=ISO-8859-1}, which reproduces the wrong answer against the unfixed comparator.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6998BinaryComparatorTest {

  /** A pair of identical non-ASCII strings: one byte under ISO-8859-1, two bytes under UTF-8. */
  private static final String NON_ASCII = "é";

  @Test
  void twoEmptyByteArraysAreEqual() {
    assertThat(BinaryComparator.equalsBytes(new byte[0], new byte[0]))
        .as("two empty byte[] are equal; the last-byte fast path used to read index -1 and throw")
        .isTrue();

    // The length check still catches a mismatched pair before the fast path.
    assertThat(BinaryComparator.equalsBytes(new byte[0], new byte[] { 1 })).isFalse();
    assertThat(BinaryComparator.equalsBytes(new byte[] { 1 }, new byte[0])).isFalse();

    // Same route through the generic entry points, which is what SQL equality reaches.
    assertThat(BinaryComparator.equals(new byte[0], new byte[0])).isTrue();
    assertThat(QueryOperatorEquals.equals(new byte[0], new byte[0]))
        .as("SQL equality over two distinct empty BINARY values must match, not fail")
        .isTrue();

    // Non-empty arrays keep the behavior the fast path was written for.
    assertThat(BinaryComparator.equalsBytes(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 })).isTrue();
    assertThat(BinaryComparator.equalsBytes(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 4 })).isFalse();
  }

  @Test
  void emptyBinaryPropertyIsMatchedBySql() throws Exception {
    TestHelper.executeInNewDatabase("Issue6998", db -> {
      db.getSchema().createDocumentType("Doc6998").createProperty("bin", Type.BINARY);

      db.transaction(() -> db.newDocument("Doc6998").set("bin", new byte[0]).save());

      // A distinct byte[0] instance, so QueryOperatorEquals' identity shortcut cannot fire and the comparison goes
      // through BinaryComparator.equalsBytes - which used to throw.
      try (final ResultSet rs = db.query("sql", "SELECT FROM Doc6998 WHERE bin = :p", Map.of("p", new byte[0]))) {
        assertThat(rs.hasNext()).as("an empty BINARY value must match another empty BINARY value").isTrue();
        rs.next();
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void compareToEncodesBothStringOperandsWithTheSameCharset() throws Exception {
    // Holds on any JVM, but only proves the fix where the default charset is not UTF-8, hence the fork below.
    assertThat(BinaryComparator.compareTo(NON_ASCII, NON_ASCII)).isZero();
    assertThat(BinaryComparator.compareTo("a" + NON_ASCII, "a" + NON_ASCII)).isZero();
    assertThat(BinaryComparator.compareTo(NON_ASCII, "f")).isNotZero();

    final String forked = runInForkedJvm(StandardCharsets.ISO_8859_1);

    assertThat(forked)
        .as("the forked JVM must actually run with a non-UTF-8 default charset, otherwise this proves nothing")
        .startsWith("charset=ISO-8859-1 ");
    assertThat(forked)
        .as("with a non-UTF-8 default charset an equal pair of non-ASCII strings must still compare equal: "
            + "encoding one operand with the platform charset and the other with the configured one made it less-than")
        .isEqualTo("charset=ISO-8859-1 compareTo=0");
  }

  /**
   * Runs {@link CharsetProbe} in a child JVM whose default charset is the given one, and returns the single line it
   * prints. {@code Charset.defaultCharset()} is decided at JVM startup, so a fork is the only way to exercise a
   * platform charset other than the one this suite runs under.
   */
  private static String runInForkedJvm(final Charset charset) throws Exception {
    final String classpath = System.getProperty("java.class.path");
    assertThat(classpath).as("the forked JVM needs this JVM's classpath").isNotBlank();

    final List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    command.add("-Dfile.encoding=" + charset.name());
    command.add("-cp");
    command.add(classpath);
    command.add(CharsetProbe.class.getName());

    // stderr is inherited rather than merged: a JVM warning on the child's stderr must not end up mixed into the
    // single line this method returns, but it still has to be visible in the build log when something goes wrong.
    final Process process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    final String output;
    try (final var in = process.getInputStream()) {
      output = new String(in.readAllBytes(), StandardCharsets.US_ASCII).trim();
    }
    assertThat(process.waitFor(60, TimeUnit.SECONDS)).as("the forked JVM must terminate; it printed: %s", output).isTrue();
    assertThat(process.exitValue()).as("the forked JVM failed; it printed: %s", output).isZero();
    return output;
  }

  /**
   * Child-JVM entry point: prints its default charset and the comparison of two identical non-ASCII strings, in
   * ASCII only so the child's own stdout encoding cannot alter the answer.
   */
  public static class CharsetProbe {
    public static void main(final String[] args) {
      System.out.println("charset=" + Charset.defaultCharset().name() //
          + " compareTo=" + BinaryComparator.compareTo(NON_ASCII, NON_ASCII));
    }
  }
}
