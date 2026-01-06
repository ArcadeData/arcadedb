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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #3091: Edge indexes become invalid in certain scenario
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeIndexCorruptionTest extends TestHelper {

  @Test
  void testEdgeIndexCorruption() {
    // Transaction #1: Create schema
    database.transaction(() -> {
      database.command("sqlscript", """
          CREATE VERTEX TYPE duct;
          CREATE VERTEX TYPE trs;
          CREATE PROPERTY duct.id STRING;
          CREATE INDEX ON duct (id) UNIQUE;
          CREATE PROPERTY trs.id STRING;
          CREATE INDEX ON trs (id) UNIQUE;
          CREATE EDGE TYPE trs_duct;
          CREATE PROPERTY trs_duct.from_id STRING;
          CREATE INDEX ON trs_duct (from_id) NOTUNIQUE;
          CREATE PROPERTY trs_duct.to_id STRING;
          CREATE INDEX ON trs_duct (to_id) NOTUNIQUE;
          CREATE PROPERTY trs_duct.swap STRING;
          CREATE PROPERTY trs_duct.order_number INTEGER;
          CREATE INDEX ON trs_duct (from_id,to_id,swap,order_number) UNIQUE;
          """);
    });

    // Transaction #2: Insert initial data
    database.transaction(() -> {
      database.command("sqlscript", """
          INSERT INTO duct (id) VALUES ('duct_1');
          INSERT INTO trs (id) VALUES ('trs_1');
          CREATE EDGE trs_duct from #4:0 to #1:0 SET from_id='trs_1', to_id='duct_1', swap='N', order_number=1;
          """);
    });

    // Transaction #3: The problematic scenario
    database.transaction(() -> {
      database.command("sqlscript", """
          INSERT INTO trs (id) VALUES ('trs_2');
          DELETE FROM trs_duct WHERE (from_id='trs_2') AND (to_id='duct_1') AND (swap='N') AND (order_number=1);
          DELETE FROM trs_duct WHERE (from_id='trs_1') AND (to_id='duct_1') AND (swap='N') AND (order_number=1);
          CREATE EDGE trs_duct from #4:1 to #1:0 SET from_id='trs_2', to_id='duct_1', swap='N', order_number=1;
          CREATE EDGE trs_duct from #4:0 to #1:0 SET from_id='trs_1', to_id='duct_1', swap='N', order_number=1;
          """);
    });

    // Check: Query should return only ONE edge, not TWO
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM trs_duct WHERE from_id='trs_1' AND to_id='duct_1'");
      long count = rs.stream().count();
      assertThat(count).as("Query should return only one edge, not multiple").isEqualTo(1);
    });

    // Transaction #4: This should NOT cause DuplicatedKeyException
    database.transaction(() -> {
      database.command("sqlscript", """
          DELETE FROM trs_duct WHERE (from_id='trs_1') AND (to_id='duct_1') AND (swap='N') AND (order_number=1);
          CREATE EDGE trs_duct from #4:0 to #1:0 SET from_id='trs_1', to_id='duct_1', swap='N', order_number=1;
          """);
    });

    // Final check: Still should have exactly the expected edges
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM trs_duct WHERE from_id='trs_1' AND to_id='duct_1'");
      long count = rs.stream().count();
      assertThat(count).as("After re-creation, query should return only one edge").isEqualTo(1);
    });
  }
}
