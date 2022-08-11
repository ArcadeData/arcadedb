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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoDBQueryTest {

  private Database database;

  @BeforeEach
  public void beginTest() {
    FileUtils.deleteRecursively(new File("./target/databases/graph"));

    database = new DatabaseFactory("./target/databases/graph").create();

    database.getSchema().createDocumentType("MongoDBCollection");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("MongoDBCollection").set("name", "Jay").set("lastName", "Miner").set("id", i).save();
    });
  }

  @AfterEach
  public void endTest() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  public void testOrderBy() {
    int i = 0;
    for (ResultSet resultset = database.query("mongo",
        "{ collection: 'MongoDBCollection', query: { $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ], $orderBy: { id: 1 } } }"); resultset.hasNext(); ++i) {
      final Result doc = resultset.next();
      assertEquals(i, (Integer) doc.getProperty("id"));
    }

    i = 9;
    for (ResultSet resultset = database.query("mongo",
        "{ collection: 'MongoDBCollection', query: { $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ], $orderBy: { id: -1 } } }"); resultset.hasNext(); --i) {
      final Result doc = resultset.next();
      assertEquals(i, (Integer) doc.getProperty("id"));
    }
  }
}
