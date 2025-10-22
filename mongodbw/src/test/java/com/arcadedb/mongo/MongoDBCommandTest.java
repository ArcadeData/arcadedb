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
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class MongoDBCommandTest {

  private Database database;

  @BeforeEach
  public void beginTest() {
    FileUtils.deleteRecursively(new File("./target/databases/graphCommand"));

    database = new DatabaseFactory("./target/databases/graphCommand").create();

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
  public void testCommandWithMap() {
    final ResultSet resultset = database.command("mongo",
        "{ collection: 'MongoDBCollection', query: { name: { $eq: 'Jay' } } }",
        Collections.emptyMap());

    int count = 0;
    while (resultset.hasNext()) {
      final Result doc = resultset.next();
      assertThat((String) doc.getProperty("name")).isEqualTo("Jay");
      count++;
    }
    assertThat((Integer) count).isEqualTo(10);
  }

  @Test
  public void testCommandWithVarargs() {
    final ResultSet resultset = database.command("mongo",
        "{ collection: 'MongoDBCollection', query: { name: { $eq: 'Jay' } } }");

    int count = 0;
    while (resultset.hasNext()) {
      final Result doc = resultset.next();
      assertThat((String) doc.getProperty("name")).isEqualTo("Jay");
      count++;
    }
    assertThat((Integer) count).isEqualTo(10);
  }

  @Test
  public void testQueryAndCommandReturnSameResults() {
    final String query = "{ collection: 'MongoDBCollection', query: { id: { $lt: 5 } } }";

    final ResultSet queryResult = database.query("mongo", query, Collections.emptyMap());
    final List<Result> queryResults = new ArrayList<>();
    while (queryResult.hasNext()) {
      queryResults.add(queryResult.next());
    }

    final ResultSet commandResult = database.command("mongo", query, Collections.emptyMap());
    final List<Result> commandResults = new ArrayList<>();
    while (commandResult.hasNext()) {
      commandResults.add(commandResult.next());
    }

    assertThat((Integer) queryResults.size()).isEqualTo(commandResults.size());
    assertThat((Integer) queryResults.size()).isEqualTo(5);
  }
}
