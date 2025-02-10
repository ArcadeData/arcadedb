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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class TraverseStatementExecutionTest extends TestHelper {
  @Test
  public void testPlainTraverse() {
    database.transaction(() -> {
      final String classPrefix = "testPlainTraverse_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      final ResultSet result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a')");

      for (int i = 0; i < 4; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testWithDepth() {
    database.transaction(() -> {
      final String classPrefix = "testWithDepth_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      final ResultSet result = database.query("sql",
          "traverse out() from (select from " + classPrefix + "V where name = 'a') WHILE $depth < 2");

      for (int i = 0; i < 2; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testMaxDepth() {
    database.transaction(() -> {
      final String classPrefix = "testMaxDepth";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      ResultSet result = database.query("sql",
          "traverse out() from (select from " + classPrefix + "V where name = 'a') MAXDEPTH 1");

      for (int i = 0; i < 2; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();

      result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a') MAXDEPTH 2");

      for (int i = 0; i < 3; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testBreadthFirst() {
    database.transaction(() -> {
      final String classPrefix = "testBreadthFirst_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix
              + "V where name = 'b')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix
              + "V where name = 'c')").close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix
              + "V where name = 'd')").close();

      final ResultSet result = database.query("sql",
          "traverse out() from (select from " + classPrefix + "V where name = 'a') STRATEGY BREADTH_FIRST");

      for (int i = 0; i < 4; i++) {
        assertThat(result.hasNext()).isTrue();
        final Result item = result.next();
        assertThat(item.getMetadata("$depth")).isEqualTo(i);
      }
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testTraverseInBatchTx() {
    database.transaction(() -> {
      String script = """
          drop type testTraverseInBatchTx_V if exists unsafe;
          create vertex type testTraverseInBatchTx_V;
          create property testTraverseInBatchTx_V.name STRING;
          drop type testTraverseInBatchTx_E if exists unsafe;
          create edge type testTraverseInBatchTx_E;
          begin;
          insert into testTraverseInBatchTx_V(name) values ('a'), ('b'), ('c');
          create edge testTraverseInBatchTx_E from (select from testTraverseInBatchTx_V where name = 'a') to (select from testTraverseInBatchTx_V where name = 'b');
          create edge testTraverseInBatchTx_E from (select from testTraverseInBatchTx_V where name = 'b') to (select from testTraverseInBatchTx_V where name = 'c');
          let top = (select * from (traverse in('testTraverseInBatchTx_E') from (select from testTraverseInBatchTx_V where name='c')) where in('testTraverseInBatchTx_E').size() == 0);
          commit;
          return $top;
          """;
      final ResultSet result = database.command("sqlscript", script);
      assertThat(result.hasNext()).isTrue();
      result.next();
      assertThat(result.hasNext()).isFalse();
      result.close();
    });
  }

  @Test
  public void testTraverseFromRID() {
    database.command("sql", "CREATE VERTEX TYPE TVtx IF NOT EXISTS");
    database.command("sql", "CREATE EDGE TYPE TEdg IF NOT EXISTS");

    database.transaction(() -> {
      RID newVtx0Id = database.command("sql", "CREATE VERTEX TVtx").next().getIdentity().get();
      RID newVtx1Id = database.command("sql", "CREATE VERTEX TVtx").next().getIdentity().get();

      Map<String, Object> params = new HashMap<>();
      params.put("fromRid", newVtx0Id);
      params.put("toRid", newVtx1Id);
      RID newEdgRid = database.command("sql", "CREATE EDGE TEdg FROM :fromRid TO :toRid", params).next().getIdentity().get();

      params.clear();
      params.put("rid", newVtx0Id);
      String traverseQuery = "SELECT FROM (TRAVERSE out('TEdg') FROM :rid MAXDEPTH 1)";
      //This also does not recognize RID parameter
      //String traverseQuery="SELECT FROM (TRAVERSE inV() FROM :rid MAXDEPTH 1)";
      database.command("sql", traverseQuery, params);
    });
  }
}
