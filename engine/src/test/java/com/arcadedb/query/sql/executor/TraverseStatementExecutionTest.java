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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class TraverseStatementExecutionTest extends TestHelper {
  @Test
  public void testPlainTraverse() {
    database.transaction(() -> {
      String classPrefix = "testPlainTraverse_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix + "V where name = 'b')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix + "V where name = 'c')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix + "V where name = 'd')")
          .close();

      ResultSet result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a')");

      for (int i = 0; i < 4; i++) {
        Assertions.assertTrue(result.hasNext());
        Result item = result.next();
        Assertions.assertEquals(i, item.getMetadata("$depth"));
      }
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testWithDepth() {
    database.transaction(() -> {
      String classPrefix = "testWithDepth_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix + "V where name = 'b')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix + "V where name = 'c')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix + "V where name = 'd')")
          .close();

      ResultSet result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a') WHILE $depth < 2");

      for (int i = 0; i < 2; i++) {
        Assertions.assertTrue(result.hasNext());
        Result item = result.next();
        Assertions.assertEquals(i, item.getMetadata("$depth"));
      }
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testMaxDepth() {
    database.transaction(() -> {
      String classPrefix = "testMaxDepth";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix + "V where name = 'b')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix + "V where name = 'c')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix + "V where name = 'd')")
          .close();

      ResultSet result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a') MAXDEPTH 1");

      for (int i = 0; i < 2; i++) {
        Assertions.assertTrue(result.hasNext());
        Result item = result.next();
        Assertions.assertEquals(i, item.getMetadata("$depth"));
      }
      Assertions.assertFalse(result.hasNext());
      result.close();

      result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a') MAXDEPTH 2");

      for (int i = 0; i < 3; i++) {
        Assertions.assertTrue(result.hasNext());
        Result item = result.next();
        Assertions.assertEquals(i, item.getMetadata("$depth"));
      }
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testBreadthFirst() {
    database.transaction(() -> {
      String classPrefix = "testBreadthFirst_";
      database.getSchema().createVertexType(classPrefix + "V");
      database.getSchema().createEdgeType(classPrefix + "E");
      database.command("sql", "create vertex " + classPrefix + "V set name = 'a'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'b'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'c'").close();
      database.command("sql", "create vertex " + classPrefix + "V set name = 'd'").close();

      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'a') to (select from " + classPrefix + "V where name = 'b')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'b') to (select from " + classPrefix + "V where name = 'c')")
          .close();
      database.command("sql",
          "create edge " + classPrefix + "E from (select from " + classPrefix + "V where name = 'c') to (select from " + classPrefix + "V where name = 'd')")
          .close();

      ResultSet result = database.query("sql", "traverse out() from (select from " + classPrefix + "V where name = 'a') STRATEGY BREADTH_FIRST");

      for (int i = 0; i < 4; i++) {
        Assertions.assertTrue(result.hasNext());
        Result item = result.next();
        Assertions.assertEquals(i, item.getMetadata("$depth"));
      }
      Assertions.assertFalse(result.hasNext());
      result.close();
    });
  }

  @Test
  public void testTraverseInBatchTx() {
    database.transaction(() -> {
      String script = "";
      script += "";

      script += "drop type testTraverseInBatchTx_V if exists unsafe;";
      script += "create vertex type testTraverseInBatchTx_V;";
      script += "create property testTraverseInBatchTx_V.name STRING;";
      script += "drop type testTraverseInBatchTx_E if exists unsafe;";
      script += "create edge type testTraverseInBatchTx_E;";

      script += "begin;";
      script += "insert into testTraverseInBatchTx_V(name) values ('a'), ('b'), ('c');";
      script += "create edge testTraverseInBatchTx_E from (select from testTraverseInBatchTx_V where name = 'a') to (select from testTraverseInBatchTx_V where name = 'b');";
      script += "create edge testTraverseInBatchTx_E from (select from testTraverseInBatchTx_V where name = 'b') to (select from testTraverseInBatchTx_V where name = 'c');";
      script += "let top = (select * from (traverse in('testTraverseInBatchTx_E') from (select from testTraverseInBatchTx_V where name='c')) where in('testTraverseInBatchTx_E').size() == 0);";
      script += "commit;";
      script += "return $top;";

      ResultSet result = database.execute("sql", script);
      Assertions.assertTrue(result.hasNext());
      Result item = result.next();
      Object val = item.getProperty("value");
      Assertions.assertTrue(val instanceof Collection);
      Assertions.assertEquals(1, ((Collection) val).size());
      result.close();
    });
  }
}
