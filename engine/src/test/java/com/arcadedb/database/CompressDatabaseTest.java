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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressDatabaseTest extends TestHelper {

  private static final int           TOTAL = 10_000;
  private              MutableVertex root;

  @Test
  public void compressDatabase() {
    final ResultSet result = database.command("sql", "check database compress");
    assertThat(result.hasNext()).isTrue();
  }

  @Override
  protected void beginTest() {
    database.command("sql", "create vertex type Person");
    database.command("sql", "create property Person.id string");
    database.command("sql", "create index on Person (id) unique");
    database.command("sql", "create edge type Knows");
    database.transaction(() -> {
      root = database.newVertex("Person").set("name", "root", "id", 0).save();
      for (int i = 1; i <= TOTAL - 1; i++) {
        final MutableVertex v = database.newVertex("Person").set("name", "test", "id", i).save();
        root.newEdge("Knows", v, true);
      }
    });
  }
}
