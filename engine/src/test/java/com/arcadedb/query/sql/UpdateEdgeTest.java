package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdateEdgeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE E");
    });
  }

  @Test
  void testUpdateEdge() {
    final RID[] rids = new RID[3];

    database.transaction(() -> {
      rids[0] = database.command("sql", "CREATE VERTEX V SET name = 'Alice'").next().getIdentity().get();
      rids[1] = database.command("sql", "CREATE VERTEX V SET name = 'Bob'").next().getIdentity().get();
      rids[2] = database.command("sql", "CREATE VERTEX V SET name = 'Charlie'").next().getIdentity().get();
      database.command("sql",
          "CREATE EDGE E FROM (SELECT FROM V WHERE name = 'Alice') TO (SELECT FROM V WHERE name = 'Bob') SET weight = 5");
      database.command("sql", "UPDATE E SET @in = " + rids[2] + " WHERE @out = " + rids[0]);
    });

    final Edge edge = database.query("sql", "SELECT FROM E").next().getEdge().get();

    assertThat(edge.getOut().toString()).isEqualTo(rids[0].toString());
    assertThat(edge.getIn().toString()).isEqualTo(rids[2].toString());
  }
}
