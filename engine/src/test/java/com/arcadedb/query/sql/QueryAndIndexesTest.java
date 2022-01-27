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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryAndIndexesTest extends TestHelper {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("V")) {
        VertexType t = database.getSchema().createVertexType("V");
        t.createProperty("name", String.class);
        t.createProperty("surname", String.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "name", "surname");
      }

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newVertex("V");
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner" + i);

        v.save();
      }
    });
  }

  @Test
  public void testEqualsFiltering() {

    database.transaction(() -> {
      Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        Result record = rs.next();
        Assertions.assertNotNull(record);

        Set<String> prop = new HashSet<>();
          prop.addAll(record.getPropertyNames());

        Assertions.assertEquals(3, record.getPropertyNames().size(), 9);
        Assertions.assertEquals(123, (int) record.getProperty("id"));
        Assertions.assertEquals("Jay", record.getProperty("name"));
        Assertions.assertEquals("Miner123", record.getProperty("surname"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(1, total.get());
    });
  }

  @Test
  public void testPartialMatchingFiltering() {

    database.transaction(() -> {
      Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        Result record = rs.next();
        Assertions.assertNotNull(record);

        Set<String> prop = new HashSet<>();
          prop.addAll(record.getPropertyNames());

        Assertions.assertEquals("Jay", record.getProperty("name"));

        total.incrementAndGet();
      }

      Assertions.assertEquals(TOT, total.get());
    });
  }
}
