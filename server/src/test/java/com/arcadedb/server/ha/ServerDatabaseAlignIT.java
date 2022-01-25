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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

public class ServerDatabaseAlignIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 3;
  }

  public ServerDatabaseAlignIT() {
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
    GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
  }

  @AfterEach
  @Override
  public void endTest() {
    super.endTest();
    FileUtils.deleteRecursively(new File("./target/config"));
    FileUtils.deleteRecursively(new File("./target/databases"));
  }

  @Test
  public void alignNotNecessary() throws InterruptedException {
    final Database database = getServer(0).getDatabase(getDatabaseName());

    database.transaction(() -> {
      final Record edge = database.iterateType(EDGE2_TYPE_NAME, true).next();

      database.deleteRecord(edge);
    });

    final ResultSet resultset = getServer(0).getDatabase(getDatabaseName()).command("sql", "align database");

    Assertions.assertTrue(resultset.hasNext());
    final Result result = resultset.next();

    Assertions.assertFalse(result.hasProperty("ArcadeDB_0"));
    Assertions.assertTrue(result.hasProperty("ArcadeDB_1"));
    Assertions.assertEquals(0, ((List<int[]>) result.getProperty("ArcadeDB_1")).size());
    Assertions.assertTrue(result.hasProperty("ArcadeDB_2"));
    Assertions.assertEquals(0, ((List<int[]>) result.getProperty("ArcadeDB_2")).size());

    // WAIT THE ALIGN IS COMPLETE BEFORE CHECKING THE DATABASES
    Thread.sleep(3000);
  }

  @Test
  public void alignNecessary() throws InterruptedException {
    final DatabaseInternal database = ((DatabaseInternal) getServer(0).getDatabase(getDatabaseName())).getEmbedded().getEmbedded();

    // EXPLICIT TX ON THE UNDERLYING DATABASE IS THE ONLY WAY TO BYPASS REPLICATED DATABASE
    database.begin();
    final Record edge = database.iterateType(EDGE1_TYPE_NAME, true).next();
    edge.delete();
    database.commit();

    try {
      checkDatabasesAreIdentical();
      Assertions.fail();
    } catch (DatabaseComparator.DatabaseAreNotIdentical e) {
      // EXPECTED
    }

    final ResultSet resultset = getServer(0).getDatabase(getDatabaseName()).command("sql", "align database");

    Assertions.assertTrue(resultset.hasNext());
    final Result result = resultset.next();

    Assertions.assertFalse(result.hasProperty("ArcadeDB_0"));
    Assertions.assertTrue(result.hasProperty("ArcadeDB_1"), "Missing response from server ArcadeDB_1: " + result.toJSON());
    Assertions.assertEquals(3, ((List<int[]>) result.getProperty("ArcadeDB_1")).size());
    Assertions.assertTrue(result.hasProperty("ArcadeDB_2"), "Missing response from server ArcadeDB_2: " + result.toJSON());
    Assertions.assertEquals(3, ((List<int[]>) result.getProperty("ArcadeDB_2")).size());

    // WAIT THE ALIGN IS COMPLETE BEFORE CHECKING THE DATABASES
    Thread.sleep(3000);
  }
}
