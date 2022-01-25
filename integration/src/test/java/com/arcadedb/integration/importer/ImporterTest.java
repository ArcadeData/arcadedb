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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ImporterTest {
  @Test
  public void importGraph() throws IOException {
    String databasePath = "target/databases/test-import-graph";

    Importer importer = new Importer(("-vertices src/test/resources/importer-vertices.csv -database " + databasePath
        + " -typeIdProperty Id -typeIdType Long -typeIdPropertyIsUnique true -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      Assertions.assertEquals(6, db.countType("Node", true));
    }

    importer = new Importer(("-edges src/test/resources/importer-edges.csv -database " + databasePath
        + " -typeIdProperty Id -typeIdType Long -edgeFromField From -edgeToField To").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      Assertions.assertEquals(6, db.countType("Node", true));
      Assertions.assertEquals("Jay", db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex().get("First Name"));
    }
    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());

  }
}
