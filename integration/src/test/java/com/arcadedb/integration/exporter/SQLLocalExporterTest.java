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
package com.arcadedb.integration.exporter;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLLocalExporterTest {
  @Test
  public void importAndExportDatabase() {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    FileUtils.deleteRecursively(new File("databases/importedFromOrientDB"));

    try (final Database database = new DatabaseFactory("databases/importedFromOrientDB").create()) {
      database.getConfiguration()
          .setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, ((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()) * 10);

      database.command("sql", "import database file://" + inputFile.getFile());

      assertThat(database.countType("Person", false)).isEqualTo(500);
      assertThat(database.countType("Friend", false)).isEqualTo(10000);

      final ResultSet result = database.command("sql", "export database file://export.jsonl.tgz with `overwrite` = true");

      final Result stats = result.next();
      assertThat((long) stats.getProperty("vertices")).isEqualTo(500L);
      assertThat((long) stats.getProperty("edges")).isEqualTo(10000L);
      assertThat(stats.<Object>getProperty("documents")).isNull();

      final File exportFile = new File("./exports/export.jsonl.tgz");
      assertThat(exportFile.exists()).isTrue();
      assertThat(exportFile.length() > 50_000).isTrue();
      exportFile.delete();
    }

    TestHelper.checkActiveDatabases();

    FileUtils.deleteRecursively(new File("databases/importedFromOrientDB"));
  }

  @Test
  public void importAndExportPartialDatabase() {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    FileUtils.deleteRecursively(new File("databases/importedFromOrientDB"));

    try (final Database database = new DatabaseFactory("databases/importedFromOrientDB").create()) {
      database.getConfiguration()
          .setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, ((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()) * 10);

      database.command("sql", "import database file://" + inputFile.getFile());

      assertThat(database.countType("Person", false)).isEqualTo(500);
      assertThat(database.countType("Friend", false)).isEqualTo(10000);

      final ResultSet result = database.command("sql", "export database file://export.jsonl.tgz with `overwrite` = true, includeTypes = Person");

      final Result stats = result.next();
      assertThat((long) stats.getProperty("vertices")).isEqualTo(500L);
      assertThat(stats.<Object>getProperty("edges")).isNull();
      assertThat(stats.<Object>getProperty("documents")).isNull();

      final File exportFile = new File("./exports/export.jsonl.tgz");
      assertThat(exportFile.exists()).isTrue();
      assertThat(exportFile.length() > 40_000).isTrue();
      exportFile.delete();
    }

    TestHelper.checkActiveDatabases();

    FileUtils.deleteRecursively(new File("databases/importedFromOrientDB"));
  }
}
