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
