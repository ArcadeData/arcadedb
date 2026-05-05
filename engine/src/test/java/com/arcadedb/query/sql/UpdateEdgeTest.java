package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

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
  void testUpdateEdgeIn() {
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

  @Test
  void testUpdateEdgeOut() {
    final RID[] rids = new RID[3];

    database.transaction(() -> {
      rids[0] = database.command("sql", "CREATE VERTEX V SET name = 'Alice'").next().getIdentity().get();
      rids[1] = database.command("sql", "CREATE VERTEX V SET name = 'Bob'").next().getIdentity().get();
      rids[2] = database.command("sql", "CREATE VERTEX V SET name = 'Charlie'").next().getIdentity().get();
      database.command("sql",
          "CREATE EDGE E FROM (SELECT FROM V WHERE name = 'Alice') TO (SELECT FROM V WHERE name = 'Bob') SET weight = 7");
      database.command("sql", "UPDATE E SET @out = " + rids[2] + " WHERE @in = " + rids[1]);
    });

    final Edge edge = database.query("sql", "SELECT FROM E").next().getEdge().get();

    assertThat(edge.getOut().toString()).isEqualTo(rids[2].toString());
    assertThat(edge.getIn().toString()).isEqualTo(rids[1].toString());
  }

  @Test
  void testUpdateEdgePreservesProperties() {
    final RID[] rids = new RID[3];

    database.transaction(() -> {
      rids[0] = database.command("sql", "CREATE VERTEX V SET name = 'Alice'").next().getIdentity().get();
      rids[1] = database.command("sql", "CREATE VERTEX V SET name = 'Bob'").next().getIdentity().get();
      rids[2] = database.command("sql", "CREATE VERTEX V SET name = 'Charlie'").next().getIdentity().get();
      database.command("sql",
          "CREATE EDGE E FROM (SELECT FROM V WHERE name = 'Alice') TO (SELECT FROM V WHERE name = 'Bob') SET weight = 42");
      database.command("sql", "UPDATE E SET @in = " + rids[2] + " WHERE @out = " + rids[0]);
    });

    final Edge edge = database.query("sql", "SELECT FROM E").next().getEdge().get();

    assertThat(edge.getInteger("weight")).isEqualTo(42);
  }

  @Test
  void testUpdateEdgeInReflectedInTraversal() {
    final RID[] rids = new RID[3];

    database.transaction(() -> {
      rids[0] = database.command("sql", "CREATE VERTEX V SET name = 'Alice'").next().getIdentity().get();
      rids[1] = database.command("sql", "CREATE VERTEX V SET name = 'Bob'").next().getIdentity().get();
      rids[2] = database.command("sql", "CREATE VERTEX V SET name = 'Charlie'").next().getIdentity().get();
      database.command("sql",
          "CREATE EDGE E FROM (SELECT FROM V WHERE name = 'Alice') TO (SELECT FROM V WHERE name = 'Bob') SET weight = 1");
      database.command("sql", "UPDATE E SET @in = " + rids[2] + " WHERE @out = " + rids[0]);
    });

    final Vertex alice = (Vertex) database.lookupByRID(rids[0], true);
    final Vertex bob = (Vertex) database.lookupByRID(rids[1], true);
    final Vertex charlie = (Vertex) database.lookupByRID(rids[2], true);

    final Iterator<Edge> aliceOut = alice.getEdges(Vertex.DIRECTION.OUT, "E").iterator();
    assertThat(aliceOut.hasNext()).isTrue();
    assertThat(aliceOut.next().getIn().toString()).isEqualTo(rids[2].toString());

    final Iterator<Edge> charlieIn = charlie.getEdges(Vertex.DIRECTION.IN, "E").iterator();
    assertThat(charlieIn.hasNext()).isTrue();
    assertThat(charlieIn.next().getOut().toString()).isEqualTo(rids[0].toString());

    assertThat(bob.getEdges(Vertex.DIRECTION.IN, "E").iterator().hasNext()).isFalse();
  }
}
