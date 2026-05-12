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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a single-property index on a LINK property must be used by SQL
 * regardless of whether the RID is written as a quoted string ("#150:5") or as an
 * unquoted RID literal (#150:5).
 *
 * Before the fix, Expression.isEarlyCalculated() returned false for RID-literal
 * expressions (the rid field was not checked), so the planner classified the
 * equality as non-index-aware and ran a full scan.
 */
class LinkIndexRidLiteralTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType library = database.getSchema().createVertexType("Library");
      library.createProperty("name", Type.STRING);

      final VertexType bo = database.getSchema().createVertexType("BusinessObject");
      bo.createProperty("_library", Type.LINK);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "BusinessObject", "_library");
    });
  }

  @Test
  void equalityOnLinkUsesIndexForQuotedStringRid() {
    final RID libRid = createFixture();

    database.transaction(() -> {
      final String explain = database.query("sql",
              "EXPLAIN SELECT FROM BusinessObject WHERE _library = \"" + libRid + "\"")
          .next().getProperty("executionPlan").toString();

      assertThat(explain).contains("FETCH FROM INDEX");

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM BusinessObject WHERE _library = \"" + libRid + "\"")) {
        assertThat(rs.stream().count()).isEqualTo(3);
      }
    });
  }

  @Test
  void equalityOnLinkUsesIndexForUnquotedRidLiteral() {
    final RID libRid = createFixture();

    database.transaction(() -> {
      final String explain = database.query("sql",
              "EXPLAIN SELECT FROM BusinessObject WHERE _library = " + libRid)
          .next().getProperty("executionPlan").toString();

      assertThat(explain).contains("FETCH FROM INDEX");

      try (final ResultSet rs = database.query("sql",
          "SELECT FROM BusinessObject WHERE _library = " + libRid)) {
        assertThat(rs.stream().count()).isEqualTo(3);
      }
    });
  }

  private RID createFixture() {
    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final var lib = database.newVertex("Library").set("name", "main").save();
      holder[0] = lib.getIdentity();
      for (int i = 0; i < 3; i++)
        database.newVertex("BusinessObject").set("_library", holder[0]).save();
      for (int i = 0; i < 5; i++) {
        final var otherLib = database.newVertex("Library").set("name", "other-" + i).save();
        database.newVertex("BusinessObject").set("_library", otherLib.getIdentity()).save();
      }
    });
    return holder[0];
  }
}
