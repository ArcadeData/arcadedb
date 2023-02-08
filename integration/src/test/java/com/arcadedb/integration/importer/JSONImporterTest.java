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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

public class JSONImporterTest {
  @Test
  public void importSingleObject() throws IOException {
    final String databasePath = "target/databases/test-import-graph";

    Importer importer = new Importer(
        ("-url file://src/test/resources/importer-one-object.json -database " + databasePath + " -documentType Food -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      Assertions.assertEquals(1, db.countType("Food", true));
    }

    TestHelper.checkActiveDatabases();
  }

  @Test
  public void importTwoObjects() throws IOException {
    final String databasePath = "target/databases/test-import-graph";

    Importer importer = new Importer(("-url file://src/test/resources/importer-two-objects.json -database " + databasePath
        + " -documentType Food -forceDatabaseCreate true -mapping {'*':[]}").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      Assertions.assertEquals(2, db.countType("Food", true));
    }

    TestHelper.checkActiveDatabases();
  }
}
