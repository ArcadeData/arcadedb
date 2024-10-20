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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CSVImporterIT {
  @Test
  public void importDocuments() {
    final String databasePath = "target/databases/test-import-documents";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "import database file://src/test/resources/importer-vertices.csv");
      assertThat(db.countType("Document", true)).isEqualTo(6);
    } finally {
      db.drop();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  public void importGraph() {
    final String databasePath = "target/databases/test-import-graph";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    Importer importer = new Importer(("-vertices src/test/resources/importer-vertices.csv -database " + databasePath
        + " -typeIdProperty Id -typeIdType Long -typeIdPropertyIsUnique true -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
    }

    importer = new Importer(("-edges src/test/resources/importer-edges.csv -database " + databasePath
        + " -typeIdProperty Id -typeIdType Long -edgeFromField From -edgeToField To").split(" "));
    importer.load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
      assertThat(db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex().get("First Name")).isEqualTo("Jay");
    }

    databaseFactory.open().drop();

    TestHelper.checkActiveDatabases();
  }

}
