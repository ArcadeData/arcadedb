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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12781: a CSV header with an empty column name (a trailing comma, a blank cell, the unnamed index column of a pandas export)
 * reached the schema as a null property name and the import died with a bare {@code NullPointerException} from
 * {@code existsProperty}. It is now refused with a message that names the column and the way out.
 */
class Issue12781CsvEmptyHeaderNameTest {
  private static final String DATABASE = "target/databases/test-import-12781";
  private static final File   FILE     = new File("target/importer-12781.csv");

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FILE.delete();
    final DatabaseFactory factory = new DatabaseFactory(DATABASE);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(DATABASE));
    TestHelper.checkActiveDatabases();
  }

  private Map<String, Object> importDocuments(final String content, final String... extraArgs) throws Exception {
    Files.writeString(FILE.toPath(), content, StandardCharsets.UTF_8);
    final String[] base = { "-documents", "file://" + FILE.getAbsolutePath(), "-database", DATABASE, "-forceDatabaseCreate", "true",
        "-documentType", "Doc", "-delimiter", "," };
    final String[] args = new String[base.length + extraArgs.length];
    System.arraycopy(base, 0, args, 0, base.length);
    System.arraycopy(extraArgs, 0, args, base.length, extraArgs.length);
    return new Importer(args).load();
  }

  private Map<String, Object> importVertices(final String content, final String... extraArgs) throws Exception {
    Files.writeString(FILE.toPath(), content, StandardCharsets.UTF_8);
    final String[] base = { "-vertices", "file://" + FILE.getAbsolutePath(), "-database", DATABASE, "-forceDatabaseCreate", "true",
        "-vertexType", "Node", "-delimiter", "," };
    final String[] args = new String[base.length + extraArgs.length];
    System.arraycopy(base, 0, args, 0, base.length);
    System.arraycopy(extraArgs, 0, args, base.length, extraArgs.length);
    return new Importer(args).load();
  }

  /** Every message along the cause chain, so the assertions read the refusal itself wherever the importer wrapped it. */
  private static String messagesOf(final Throwable thrown) {
    final StringBuilder messages = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause())
      messages.append(t.getMessage()).append('\n');
    return messages.toString();
  }

  @Test
  void aHeaderReadFromTheFileNamesOnlyTheSettingItsRouteReads() {
    assertThatThrownBy(() -> importDocuments("a,,b\n1,2,3\n")).satisfies(
        e -> assertThat(messagesOf(e)).contains("first line of the file").contains("-documentsHeader setting")
            .doesNotContain("-verticesHeader").doesNotContain("-edgesHeader"));

    assertThatThrownBy(() -> importVertices("a,,b\n1,2,3\n")).satisfies(
        e -> assertThat(messagesOf(e)).contains("no name for column 2 of 3").contains("-verticesHeader setting")
            .doesNotContain("-documentsHeader").doesNotContain("-edgesHeader"));
  }

  @Test
  void aSuppliedHeaderIsNotToldToSupplyAHeader() {
    assertThatThrownBy(() -> importDocuments("1,2,3\n4,5,6\n", "-documentsHeader", "a,,b")).satisfies(
        e -> assertThat(messagesOf(e)).contains("supplied with the -documentsHeader setting").contains("no name for column 2 of 3")
            .doesNotContain("first line of the file").doesNotContain("name it in the file").doesNotContain("supply the column names"));
  }

  @Test
  void aTrailingSeparatorOnASuppliedHeaderIsRefusedLikeTheSameLineInTheFile() {
    // split() without a limit drops trailing empty strings, so this used to pass as two columns
    assertThatThrownBy(() -> importDocuments("1,2,3\n4,5,6\n", "-documentsHeader", "a,b,")).hasStackTraceContaining(
        "no name for column 3 of 3");
    assertThatThrownBy(() -> importVertices("1,2,3\n4,5,6\n", "-verticesHeader", "a,,")).hasStackTraceContaining(
        "supplied with the -verticesHeader setting has no name for column 2 of 3");
  }

  @Test
  void aBlankSuppliedHeaderIsRefused() {
    assertThatThrownBy(() -> importDocuments("1\n2\n", "-documentsHeader", " ")).hasStackTraceContaining(
        "no name for column 1 of 1");
  }

  @Test
  void anEmptyHeaderNameIsRefusedNamingTheColumn() {
    assertThatThrownBy(() -> importDocuments("a,,b\n1,2,3\n4,5,6\n")).isInstanceOf(ImportException.class)
        .hasStackTraceContaining("no name for column 2 of 3").hasStackTraceContaining("-documentsHeader");
  }

  @Test
  void aTrailingCommaOnTheHeaderIsRefused() {
    assertThatThrownBy(() -> importDocuments("a,b,\n1,2,3\n4,5,6\n")).hasStackTraceContaining("no name for column 3 of 3");
  }

  @Test
  void theUnnamedIndexColumnOfAPandasExportIsRefused() {
    assertThatThrownBy(() -> importDocuments(",a,b\n0,1,2\n1,3,4\n")).hasStackTraceContaining("no name for column 1 of 3");
  }

  @Test
  void aBlankHeaderNameIsRefused() {
    assertThatThrownBy(() -> importDocuments("a, ,b\n1,2,3\n")).hasStackTraceContaining("no name for column 2 of 3");
  }

  @Test
  void aSuppliedHeaderMakesTheFileImportable() throws Exception {
    final Map<String, Object> report = importDocuments(",a,b\n0,1,2\n1,3,4\n", "-documentsHeader", "idx,a,b", "-documentsSkipEntries", "1");

    assertThat(report).containsEntry("createdDocuments", 2L);
    try (final Database db = new DatabaseFactory(DATABASE).open()) {
      assertThat(db.countType("Doc", true)).isEqualTo(2);
      assertThat(db.query("sql", "select from Doc where idx = 1").next().<Number>getProperty("a").intValue()).isEqualTo(3);
    }
  }

  @Test
  void aSuppliedHeaderWithAnEmptyNameIsRefused() {
    assertThatThrownBy(() -> importDocuments("1,2,3\n4,5,6\n", "-documentsHeader", "a,,b")).hasStackTraceContaining(
        "no name for column 2 of 3");
  }

  @Test
  void aWellFormedHeaderStillImports() throws Exception {
    final Map<String, Object> report = importDocuments("a,b,c\n1,2,3\n4,5,6\n");

    assertThat(report).containsEntry("createdDocuments", 2L);
  }
}
