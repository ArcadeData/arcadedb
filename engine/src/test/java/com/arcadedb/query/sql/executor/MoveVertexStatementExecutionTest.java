package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * original @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdatabase.com)
 * Ported by @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MoveVertexStatementExecutionTest extends TestHelper {

  @Test
  public void testMoveVertex() {
    String vertexClassName1 = "testMoveVertexV1";
    String vertexClassName2 = "testMoveVertexV2";
    String edgeClassName = "testMoveVertexE";
    database.getSchema().createVertexType(vertexClassName1);
    database.getSchema().createVertexType(vertexClassName2);
    database.getSchema().createEdgeType(edgeClassName);

    database.setAutoTransaction(true);

    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'a'");
    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'b'");
    database.command("sql",
        "create edge "
            + edgeClassName
            + " from (select from "
            + vertexClassName1
            + " where name = 'a' ) to (select from "
            + vertexClassName1
            + " where name = 'b' )");

    database.command("sql",
        "MOVE VERTEX (select from "
            + vertexClassName1
            + " where name = 'a') to type:" + vertexClassName2);
    ResultSet rs = database.query("sql", "select from " + vertexClassName1);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();

    rs = database.query("sql", "select from " + vertexClassName2);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();

    rs = database.query("sql", "select expand(out()) from " + vertexClassName2);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();

    rs = database.query("sql", "select expand(in()) from " + vertexClassName1);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testMoveVertexBatch() {
    String vertexClassName1 = "testMoveVertexBatchV1";
    String vertexClassName2 = "testMoveVertexBatchV2";
    String edgeClassName = "testMoveVertexBatchE";
    database.getSchema().createVertexType(vertexClassName1);
    database.getSchema().createVertexType(vertexClassName2);
    database.getSchema().createEdgeType(edgeClassName);

    database.setAutoTransaction(true);

    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'a'");
    database.command("sql", "create vertex " + vertexClassName1 + " set name = 'b'");
    database.command("sql",
        "create edge "
            + edgeClassName
            + " from (select from "
            + vertexClassName1
            + " where name = 'a' ) to (select from "
            + vertexClassName1
            + " where name = 'b' )");

    database.command("sql",
        "MOVE VERTEX (select from "
            + vertexClassName1
            + " where name = 'a') to type:" + vertexClassName2 + " BATCH 2");
    ResultSet rs = database.query("sql", "select from " + vertexClassName1);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();

    rs = database.query("sql", "select from " + vertexClassName2);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();

    rs = database.query("sql", "select expand(out()) from " + vertexClassName2);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();

    rs = database.query("sql", "select expand(in()) from " + vertexClassName1);
    Assertions.assertTrue(rs.hasNext());
    rs.next();
    Assertions.assertFalse(rs.hasNext());
    rs.close();
  }
}
