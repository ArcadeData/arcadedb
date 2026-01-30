package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CreateEdgeParametersTest extends TestHelper {

  @Test
  void createEdgeWithPositionalParameters() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V").save();
      final MutableVertex v2 = database.newVertex("V").save();

//      System.out.println("v1: " + v1);
//      System.out.println("v2: " + v2);

      final ResultSet result = database.command("sql", "create edge E from ? to ?", v1, v2);
//      assertThat(result).isNotNull();
      assertThat(result.hasNext()).isTrue();
//      System.out.println("Edge created successfully!");
    });
  }
}
