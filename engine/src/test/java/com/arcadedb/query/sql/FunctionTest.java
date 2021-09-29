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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class FunctionTest extends TestHelper {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database database) {
        if (!database.getSchema().existsType("V"))
          database.getSchema().createVertexType("V");

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newVertex("V");
          v.set("id", i);
          v.set("name", "Jay");
          v.set("surname", "Miner" + i);

          v.save();
        }
      }
    });
  }

  @Test
  public void testCountFunction() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT count(*) as count FROM V WHERE id < :id", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          assertNotNull(record);
          assertFalse(record.getIdentity().isPresent());
          assertEquals(10, ((Number) record.getProperty("count")).intValue());
          counter.incrementAndGet();
        }
        assertEquals(1, counter.get());
      }
    });
  }

  @Test
  public void testAvgFunction() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        params.put(":id", 10);
        ResultSet rs = db.command("SQL", "SELECT avg(id) as avg FROM V WHERE id < :id", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          assertNotNull(record);
          assertFalse(record.getIdentity().isPresent());
          assertEquals(4, ((Number) record.getProperty("avg")).intValue());
          counter.incrementAndGet();
        }
        assertEquals(1, counter.get());
      }
    });
  }

  @Test
  public void testMaxFunction() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        ResultSet rs = db.command("SQL", "SELECT max(id) as max FROM V", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          assertNotNull(record);
          assertFalse(record.getIdentity().isPresent());
          assertEquals(TOT - 1, ((Number) record.getProperty("max")).intValue());
          counter.incrementAndGet();
        }
        assertEquals(1, counter.get());
      }
    });
  }

  @Test
  public void testMinFunction() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute(Database db) {
        Map<String, Object> params = new HashMap<>();
        ResultSet rs = db.command("SQL", "SELECT min(id) as min FROM V", params);

        final AtomicInteger counter = new AtomicInteger();
        while (rs.hasNext()) {
          Result record = rs.next();
          assertNotNull(record);
          assertFalse(record.getIdentity().isPresent());
          assertEquals(0, ((Number) record.getProperty("min")).intValue());
          counter.incrementAndGet();
        }
        assertEquals(1, counter.get());
      }
    });
  }
}
