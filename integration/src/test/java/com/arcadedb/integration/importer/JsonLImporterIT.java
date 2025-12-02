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

import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JsonLImporterIT {

  private final static String DATABASE_PATH = "target/databases/arcadedb-jsonl-importer";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

  }

  @Test
  void importDatabaseProgrammatically() {
    var databaseDirectory = new File(DATABASE_PATH);

    var inputFile = getClass().getClassLoader().getResource("arcadedb-export.jsonl.tgz");

    var importer = new Importer(
        ("-url " + inputFile.getFile() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
    Map<String, Object> loaded = importer.load();

    assertThat(databaseDirectory.exists()).isTrue();

    checkImportedDatabase();

  }

  @Test
  void importDatabaseBySql() {

    var databaseDirectory = new File(DATABASE_PATH);

    var inputFile = getClass().getClassLoader().getResource("arcadedb-export.jsonl.tgz");

    var db = new DatabaseFactory(DATABASE_PATH).create();

    db.command("sql", "import database file://" + inputFile.getFile());

    db.close();

    checkImportedDatabase();
  }

  private static void checkImportedDatabase() {
    try (var db = new DatabaseFactory(DATABASE_PATH).open()) {

      var schema = db.getSchema();

      //scheck schema
      assertThat(schema.getDateFormat()).isEqualTo("yyyy-MM-dd");
      assertThat(schema.getDateTimeFormat()).isEqualTo("yyyy-MM-dd HH:mm:ss");
      assertThat(schema.getZoneId()).isEqualTo(ZoneId.of("Europe/Rome"));

      //check types
      assertThat(schema.getTypes()).hasSize(2);
      assertThat(schema.getType("Person")).isNotNull()
          .satisfies(type -> {
            assertThat(type.getProperty("id").getType()).isEqualTo(Type.INTEGER);
            assertThat(type.getIndexesByProperties("id").getFirst())
                .satisfies(index -> {
                  assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
                  assertThat(index.getNullStrategy()).isEqualTo(NULL_STRATEGY.SKIP);
                  assertThat(index.isUnique()).isTrue();
                });
          });
      assertThat(schema.getType("Friend")).isNotNull()
          .satisfies(type -> {
            assertThat(type.getProperty("id").getType()).isEqualTo(Type.INTEGER);
            assertThat(type.getIndexesByProperties("id").getFirst())
                .satisfies(index -> {
                  assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
                  assertThat(index.getNullStrategy()).isEqualTo(NULL_STRATEGY.SKIP);
                  assertThat(index.isUnique()).isTrue();
                });
          });

      //check vertices
      assertThat(db.countType("Person", true)).isEqualTo(500);

      //check edges
      assertThat(db.countType("Friend", true)).isEqualTo(10000);

    }
  }



}
