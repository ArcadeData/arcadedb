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

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryAndIndexesTest extends TestHelper {
  private static final int TOT = 10000;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType("V")) {
        final VertexType t = database.getSchema().createVertexType("V");
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
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name AND surname = :surname", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();

        final Set<String> prop = new HashSet<>();
          prop.addAll(record.getPropertyNames());

        assertThat(record.getPropertyNames().size()).isEqualTo(3);
        assertThat((int) record.getProperty("id")).isEqualTo(123);
        assertThat(record.<String>getProperty("name")).isEqualTo("Jay");
        assertThat(record.<String>getProperty("surname")).isEqualTo("Miner123");

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(1);
    });
  }

  @Test
  public void testPartialMatchingFiltering() {

    database.transaction(() -> {
      final Map<String, Object> params = new HashMap<>();
      params.put(":name", "Jay");
      params.put(":surname", "Miner123");
      final ResultSet rs = database.command("SQL", "SELECT FROM V WHERE name = :name", params);

      final AtomicInteger total = new AtomicInteger();
      while (rs.hasNext()) {
        final Result record = rs.next();
        assertThat(record).isNotNull();

        final Set<String> prop = new HashSet<>();
          prop.addAll(record.getPropertyNames());

        assertThat(record.<String>getProperty("name")).isEqualTo("Jay");

        total.incrementAndGet();
      }

      assertThat(total.get()).isEqualTo(TOT);
    });
  }
}
