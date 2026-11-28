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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12772: a Word2Vec or GloVe text file with a blank line (the empty last line of a text file first of all) aborted the whole
 * import with a {@code NoSuchElementException}, and a doubled separator with {@code NumberFormatException: empty String}.
 */
class Issue12772EmbeddingsBlankLineTest {
  private static final String DATABASE = "target/databases/test-import-12772";

  private Map<String, Object> importText(final String content) throws Exception {
    final File source = new File("target/importer-12772.txt");
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    FileUtils.deleteRecursively(new File(DATABASE));
    try {
      return new Importer(new String[] { "-url", "file://" + source.getAbsolutePath(), "-database", DATABASE, "-forceDatabaseCreate",
          "true", "-vertexType", "Word", "-vectorProperty", "vector", "-idProperty", "name" }).load();
    } finally {
      source.delete();
    }
  }

  private void assertWords(final int expected, final String... names) {
    try (final Database db = new DatabaseFactory(DATABASE).open()) {
      assertThat(db.countType("Word", true)).isEqualTo(expected);
      assertThat(db.getSchema().getIndexByName("Word[vector]").countEntries()).isEqualTo(expected);
      for (final String name : names)
        assertThat(db.query("sql", "select from Word where name = ?", name).next().<float[]>getProperty("vector")).containsExactly(
            name.equals("alpha") ? new float[] { 1, 2, 3 } : new float[] { 4, 5, 6 });
    }
  }

  private static void cleanup() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(DATABASE));
    TestHelper.checkActiveDatabases();
  }

  @Test
  void aTrailingEmptyLineIsNoEmbedding() throws Exception {
    try {
      final Map<String, Object> report = importText("2 3\nalpha 1 2 3\nbeta 4 5 6\n\n");
      assertWords(2, "alpha", "beta");
      // THE HEADER AND THE BLANK LINE ARE SKIPPED RECORDS: PARSED = CREATED + SKIPPED
      assertThat(report.get("parsedRecords")).isEqualTo(4L);
      assertThat(report.get("createdRecords")).isEqualTo(2L);
      assertThat(report.get("skippedRecords")).isEqualTo(2L);
    } finally {
      cleanup();
    }
  }

  @Test
  void blankAndWhitespaceOnlyLinesAnywhereAreSkipped() throws Exception {
    try {
      // THE SECOND LINE STAYS AN EMBEDDING: THE FORMAT IS SNIFFED FROM THE FIRST TWO LINES
      importText("2 3\nalpha 1 2 3\n\n   \n\t\nbeta 4 5 6\n\n\n");
      assertWords(2, "alpha", "beta");
    } finally {
      cleanup();
    }
  }

  @Test
  void aBlankLineRightAfterTheHeaderDoesNotHideTheFormat() throws Exception {
    for (final String blank : new String[] { "\n", "   \n", "\t\n\n  \n" }) {
      try {
        // THE FORMAT IS SNIFFED FROM THE HEADER AND THE NEXT LINE WITH CONTENT: A CSV FALLBACK WOULD NEVER REACH THE EMBEDDINGS
        importText("2 3\n" + blank + "alpha 1 2 3\nbeta 4 5 6\n");
        assertWords(2, "alpha", "beta");
      } finally {
        cleanup();
      }
    }
  }

  @Test
  void aDoubledSeparatorIsNoComponent() throws Exception {
    try {
      importText("2 3\nalpha 1  2   3\nbeta  4 5 6 \n");
      assertWords(2, "alpha", "beta");
    } finally {
      cleanup();
    }
  }

  @Test
  void aLeadingSeparatorIsNotTheWord() throws Exception {
    try {
      // THE WORD IS THE FIRST TOKEN WITH CONTENT, NOT THE EMPTY ONE IN FRONT OF A LEADING SEPARATOR
      importText("2 3\n alpha 1 2 3\n   beta 4 5 6\n");
      assertWords(2, "alpha", "beta");
    } finally {
      cleanup();
    }
  }

  @Test
  void theFormatIsSniffedFromTheTokensTheImporterReads() throws Exception {
    // THE SNIFFING COUNTS THE FIELDS OF THE FIRST TWO LINES. COUNTING THE EMPTY ONES BETWEEN SEPARATORS SENT THESE FILES TO THE
    // CSV FALLBACK, WHICH NEVER REACHES THE EMBEDDINGS: A WORD2VEC HEADER WITH A DOUBLED SEPARATOR, GLOVE ROWS (NO HEADER)
    // SPACED DIFFERENTLY, A GLOVE ROW WITH A LEADING OR A TRAILING SEPARATOR
    for (final String content : new String[] { "2  3\nalpha 1 2 3\nbeta 4 5 6\n", "alpha 1 2 3\nbeta  4 5 6\n",
        " alpha 1 2 3\nbeta 4 5 6\n", "alpha 1 2 3\nbeta 4  5 6 \n" }) {
      try {
        importText(content);
        assertWords(2, "alpha", "beta");
      } finally {
        cleanup();
      }
    }
  }

  @Test
  void theTokensOfALineAreCountedWithoutTheEmptyOnes() {
    assertThat(SourceDiscovery.countSpaceSeparatedTokens("")).isZero();
    assertThat(SourceDiscovery.countSpaceSeparatedTokens("   ")).isZero();
    assertThat(SourceDiscovery.countSpaceSeparatedTokens("2 3")).isEqualTo(2);
    assertThat(SourceDiscovery.countSpaceSeparatedTokens("2  3")).isEqualTo(2);
    assertThat(SourceDiscovery.countSpaceSeparatedTokens(" alpha 1  2 3 ")).isEqualTo(4);
    assertThat(SourceDiscovery.countSpaceSeparatedTokens("alpha 1 2 3 \r")).isEqualTo(4);
  }

  @Test
  void aMalformedRowIsStillRefused() throws Exception {
    try {
      assertThatThrownBy(() -> importText("2 3\nalpha 1 2 3\nbeta 4 5\n")).hasStackTraceContaining("Vector dimension does not match");
    } finally {
      cleanup();
    }
    try {
      assertThatThrownBy(() -> importText("2 3\nalpha 1 2 3\nbeta 4 5 abc\n")).hasStackTraceContaining("NumberFormatException");
    } finally {
      cleanup();
    }
  }
}
