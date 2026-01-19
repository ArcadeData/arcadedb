package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CreateEdgeParametersTest extends TestHelper {

  @Test
  public void testCreateEdgeWithPositionalParameters() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V").save();
      final MutableVertex v2 = database.newVertex("V").save();

      System.out.println("v1: " + v1);
      System.out.println("v2: " + v2);

      final var result = database.command("sql", "create edge E from ? to ?", v1, v2);
      assertNotNull(result);
      assertTrue(result.hasNext());
      System.out.println("Edge created successfully!");
    });
  }
}
