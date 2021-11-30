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
 */
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckDatabaseTest extends TestHelper {
  @Test
  public void checkDatabase() {
    final ResultSet result = database.command("sql", "check database");
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(100_000, (Long) row.getProperty("totalRecords"));
      Assertions.assertEquals(100_000, (Long) row.getProperty("totalVertices"));
      Assertions.assertEquals(100_000, (Long) row.getProperty("totalActiveRecords"));
    }
  }

  @Test
  public void checkBuckets() {
    final ResultSet result = database.command("sql", "check database type 'Person'");
    while (result.hasNext()) {
      final Result row = result.next();

      Assertions.assertEquals("check database", row.getProperty("operation"));
      Assertions.assertEquals(100_000, (Long) row.getProperty("totalRecords"));
      Assertions.assertEquals(100_000, (Long) row.getProperty("totalVertices"));
      Assertions.assertEquals(100_000, (Long) row.getProperty("totalActiveRecords"));
    }
  }

  @Override
  protected void beginTest() {
    database.command("sql", "create vertex type Person");
    database.transaction(() -> {
      for (int i = 0; i < 100_000; i++) {
        database.newVertex("Person").set("name", "test", "age", 33).save();
      }
    });
  }
}
