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

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class MongoDBQueryTest {

  private Database database;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File("./target/databases/graph"));

    database = new DatabaseFactory("./target/databases/graph").create();

    database.getSchema().createDocumentType("MongoDBCollection");

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("MongoDBCollection").set("name", "Jay").set("lastName", "Miner").set("id", i).save();
    });
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  void orderBy() {
    int i = 0;
    for (final ResultSet resultset = database.query("mongo",
        "{ collection: 'MongoDBCollection', query: { $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ], $orderBy: { id: 1 } } }"); resultset.hasNext(); ++i) {
      final Result doc = resultset.next();
      assertThat((Integer) doc.getProperty("id")).isEqualTo(i);
    }

    i = 9;
    for (final ResultSet resultset = database.query("mongo",
        "{ collection: 'MongoDBCollection', query: { $and: [ { name: { $eq: 'Jay' } }, { lastName: { $exists: true } }, { lastName: { $eq: 'Miner' } }, { lastName: { $ne: 'Miner22' } } ], $orderBy: { id: -1 } } }"); resultset.hasNext(); --i) {
      final Result doc = resultset.next();
      assertThat((Integer) doc.getProperty("id")).isEqualTo(i);
    }
  }

  /**
   * Regression test for issue #6748 (2): {@code numberToReturn} used to be read only when {@code numberToSkip} was
   * also present in the request JSON, so a {@code numberToReturn}-only request silently ignored the limit and
   * returned every matching document instead.
   */
  @Test
  void numberToReturnIsHonoredWithoutNumberToSkip() {
    int count = 0;
    for (final ResultSet resultset = database.query("mongo",
        "{ collection: 'MongoDBCollection', numberToReturn: 3, query: { name: { $eq: 'Jay' } } }"); resultset.hasNext(); ) {
      resultset.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  /**
   * Regression test for issue #6748 (2): a {@code numberToSkip}-only request used to read {@code numberToReturn}
   * under the wrong JSON key, so {@code getInt} threw {@code JSONException} on the missing key and failed the
   * whole query instead of treating the limit as unset.
   */
  @Test
  void numberToSkipAloneDoesNotThrow() {
    int count = 0;
    for (final ResultSet resultset = database.query("mongo",
        "{ collection: 'MongoDBCollection', numberToSkip: 2, query: { name: { $eq: 'Jay' } } }"); resultset.hasNext(); ) {
      resultset.next();
      count++;
    }
    assertThat(count).isEqualTo(8);
  }
}
