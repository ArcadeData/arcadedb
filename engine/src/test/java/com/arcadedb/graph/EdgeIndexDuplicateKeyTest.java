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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

/**
 * Test for issue #3097: Edge indexes become invalid in certain scenario #2
 * Reproduces DuplicatedKeyException when deleting and recreating the same edge multiple times.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeIndexDuplicateKeyTest extends TestHelper {

  @Test
  void testEdgeDeleteAndRecreateMultipleTimes() {
    // Transaction #1: Create schema
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE duct");
      database.command("sql", "CREATE VERTEX TYPE trs");
      database.command("sql", "CREATE PROPERTY duct.id STRING");
      database.command("sql", "CREATE INDEX ON duct (id) UNIQUE");
      database.command("sql", "CREATE PROPERTY trs.id STRING");
      database.command("sql", "CREATE INDEX ON trs (id) UNIQUE");
      database.command("sql", "CREATE EDGE TYPE trs_duct");
      database.command("sql", "CREATE PROPERTY trs_duct.from_id STRING");
      database.command("sql", "CREATE INDEX ON trs_duct (from_id) NOTUNIQUE");
      database.command("sql", "CREATE PROPERTY trs_duct.to_id STRING");
      database.command("sql", "CREATE INDEX ON trs_duct (to_id) NOTUNIQUE");
      database.command("sql", "CREATE PROPERTY trs_duct.swap STRING");
      database.command("sql", "CREATE PROPERTY trs_duct.order_number INTEGER");
      database.command("sql", "CREATE INDEX ON trs_duct (from_id,to_id,swap,order_number) UNIQUE");
    });

    // Transaction #2: Insert vertices and create edge
    database.transaction(() -> {
      database.command("sql", "INSERT INTO duct (id) VALUES ('duct_1')");
      database.command("sql", "INSERT INTO trs (id) VALUES ('trs_1')");
      database.command("sql",
          "CREATE EDGE trs_duct from (SELECT FROM trs WHERE id='trs_1') to (SELECT FROM duct WHERE id='duct_1') " +
          "SET from_id='trs_1', to_id='duct_1', swap='N', order_number=1");
    });

    // Transaction #3: Delete and recreate edge (first time - should work)
    database.transaction(() -> {
      database.command("sql",
          "DELETE FROM trs_duct WHERE (from_id='trs_1') AND (to_id='duct_1') AND (swap='N') AND (order_number=1)");
      database.command("sql",
          "CREATE EDGE trs_duct from (SELECT FROM trs WHERE id='trs_1') to (SELECT FROM duct WHERE id='duct_1') " +
          "SET from_id='trs_1', to_id='duct_1', swap='N', order_number=1");
    });

    // Transaction #4: Delete and recreate edge (second time - this should NOT throw DuplicatedKeyException)
    database.transaction(() -> {
      database.command("sql",
          "DELETE FROM trs_duct WHERE (from_id='trs_1') AND (to_id='duct_1') AND (swap='N') AND (order_number=1)");
      database.command("sql",
          "CREATE EDGE trs_duct from (SELECT FROM trs WHERE id='trs_1') to (SELECT FROM duct WHERE id='duct_1') " +
          "SET from_id='trs_1', to_id='duct_1', swap='N', order_number=1");
    });

    // If we got here without exception, the test passes
  }
}
