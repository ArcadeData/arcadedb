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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

public class SQLRemoteImporterIT {
  @Test
  public void importOrientDB() {
    FileUtils.deleteRecursively(new File("./target/databases/importedFromOrientDB"));

    try (final Database database = new DatabaseFactory("./target/databases/importedFromOrientDB").create()) {

      //database.command("sql", "import database https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/MovieRatings.gz");
      database.command("sql", "import database https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/GratefulDeadConcerts.gz");

      Assertions.assertEquals(809, database.countType("V", false));
      Assertions.assertEquals(7047, database.countType("followed_by", false));
      Assertions.assertEquals(501, database.countType("sung_by", false));
      Assertions.assertEquals(501, database.countType("written_by", false));
    }

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(new File("./target/databases/importedFromOrientDB"));
  }
}
