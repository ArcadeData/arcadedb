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
import com.arcadedb.database.Document;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6811: {@code SourceDiscovery.analyzeSourceContent()} used to write {@code null} into
 * {@code settings.options["delimiter"]} for every CSV source whose entity type carries no per-source delimiter,
 * destroying the value the user had supplied via {@code -delimiter} / {@code WITH delimiter = ';'}. A semicolon CSV
 * was then parsed as a single column, and the import still reported success.
 */
class Issue6811DelimiterOptionTest {

  /**
   * The reported repro: {@code IMPORT DATABASE file://...csv WITH delimiter = ';'}. The main {@code -url} is routed
   * as {@code EntityType.DATABASE}, which has no per-source delimiter setting at all, so before the fix the user's
   * {@code ;} was overwritten with {@code null} and the parser fell back to a comma.
   */
  @Test
  void importDatabaseHonorsTheDelimiterOption() {
    final String databasePath = "target/databases/test-import-6811-database";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", """
          IMPORT DATABASE file://src/test/resources/importer-delimiter-semicolon.csv
          WITH delimiter = ';'
          """);

      assertThat(db.countType("Document", true)).isEqualTo(3);

      final Iterator<Document> it = db.iterateType("Document", true);
      final Document first = it.next();

      // BEFORE THE FIX THE WHOLE LINE LANDED IN A SINGLE PROPERTY LITERALLY NAMED "id;name;age"
      assertThat(first.getPropertyNames()).containsExactlyInAnyOrder("id", "name", "age");
      assertThat(db.getSchema().getType("Document").getPropertyNames()).doesNotContain("id;name;age");

      assertThat(db.query("sql", "SELECT FROM Document WHERE id = 1").next().<Object>getProperty("name")).isEqualTo("Alice");
      assertThat(db.query("sql", "SELECT FROM Document WHERE id = 3").next().<Object>getProperty("age")).isEqualTo(28L);
    } finally {
      db.drop();
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * Same defect through the command-line/API form: {@code -delimiter} was equally destroyed, and there was no
   * alternative for a plain {@code -url} source.
   */
  @Test
  void urlImportHonorsTheDelimiterOption() {
    final String databasePath = "target/databases/test-import-6811-url";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    new Importer(new String[] {//
        "-url", "src/test/resources/importer-delimiter-semicolon.csv",//
        "-database", databasePath,//
        "-delimiter", ";",//
        "-forceDatabaseCreate", "true" }).load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Document", true)).isEqualTo(3);

      final Iterator<Document> it = db.iterateType("Document", true);
      assertThat(it.next().getPropertyNames()).containsExactlyInAnyOrder("id", "name", "age");
    }

    databaseFactory.open().drop();
    TestHelper.checkActiveDatabases();
  }

  /**
   * The generic {@code -delimiter} also has to reach a source that has its own setting name but no value for it,
   * here the vertices file.
   */
  @Test
  void genericDelimiterAppliesToVertices() {
    final String databasePath = "target/databases/test-import-6811-generic";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    new Importer(new String[] {//
        "-vertices", "src/test/resources/importer-vertices-semicolon.csv",//
        "-database", databasePath,//
        "-delimiter", ";",//
        "-typeIdProperty", "Id",//
        "-typeIdType", "Long",//
        "-typeIdUnique", "true",//
        "-forceDatabaseCreate", "true" }).load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
      assertThat(db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex().<Object>get("First Name")).isEqualTo("Jay");
    }

    databaseFactory.open().drop();
    TestHelper.checkActiveDatabases();
  }

  /**
   * The other half of the precedence rule: a per-source delimiter wins over the generic one, so the semicolon
   * vertices file is still parsed on {@code ;} even though the generic setting says comma. A fallback applied in the
   * wrong order would parse the whole line as one column here and lose the {@code First Name} property.
   */
  @Test
  void perSourceDelimiterWinsOverTheGenericOne() {
    final String databasePath = "target/databases/test-import-6811-precedence";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    new Importer(new String[] {//
        "-vertices", "src/test/resources/importer-vertices-semicolon.csv",//
        "-verticesDelimiter", ";",//
        "-database", databasePath,//
        "-delimiter", ",",//
        "-typeIdProperty", "Id",//
        "-typeIdType", "Long",//
        "-typeIdUnique", "true",//
        "-forceDatabaseCreate", "true" }).load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
      assertThat(db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex().<Object>get("First Name")).isEqualTo("Jay");
    }

    databaseFactory.open().drop();
    TestHelper.checkActiveDatabases();
  }

  /**
   * Guards the fix itself: the resolved delimiter is per source, so a semicolon vertices file followed by a
   * comma-separated edges file in the SAME import must not let the vertices' {@code ;} leak into the edges parse.
   */
  @Test
  void aPerSourceDelimiterDoesNotLeakIntoTheNextSource() {
    final String databasePath = "target/databases/test-import-6811-noleak";
    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    new Importer(new String[] {//
        "-vertices", "src/test/resources/importer-vertices-semicolon.csv",//
        "-verticesDelimiter", ";",//
        "-edges", "src/test/resources/importer-edges.csv",//
        "-database", databasePath,//
        "-typeIdProperty", "Id",//
        "-typeIdType", "Long",//
        "-typeIdUnique", "true",//
        "-edgeFromField", "From",//
        "-edgeToField", "To",//
        "-forceDatabaseCreate", "true" }).load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
      assertThat(db.countType("Relationship", true)).isEqualTo(3);
      assertThat(db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex().<Object>get("First Name")).isEqualTo("Jay");
    }

    databaseFactory.open().drop();
    TestHelper.checkActiveDatabases();
  }
}
