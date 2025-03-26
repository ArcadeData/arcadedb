package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class TestVertexDelete extends TestHelper {

  @Test
  public void testFullEdgeDeletion() {
    createSchema(database);
    for (int i = 0; i < 100; i++) {
      List<Vertex> vlist = new ArrayList<>();
      database.transaction(() -> {
        vlist.addAll(createTree(database));
        deleteTree(vlist);
      });

      database.transaction(() -> {
        var v1c = database.countType("v1", false);
        var pc = database.countType("hasParent", false);
        assertThat(v1c).isEqualTo(0);
        assertThat(pc).isEqualTo(0);
      });
    }
  }

  private static void createSchema(Database db) {
    db.transaction(() -> {
      db.getSchema().createVertexType("v1");
      db.getSchema().createEdgeType("hasParent");
    });
  }

  // create tree of vertices all connected by edges
  private static List<Vertex> createTree(Database db) {
    var p1 = db.newVertex("v1").save();
    var p11 = db.newVertex("v1").save();
    p11.newEdge("hasParent", p1).save();
    var p12 = db.newVertex("v1").save();
    p12.newEdge("hasParent", p1).save();
    var n1 = db.newVertex("v1").save();
    n1.newEdge("hasParent", p1).save();
    var n2 = db.newVertex("v1").save();
    n2.newEdge("hasParent", p11).save();
    var n3 = db.newVertex("v1").save();
    n3.newEdge("hasParent", p11).save();
    var n4 = db.newVertex("v1").save();
    n4.newEdge("hasParent", p12).save();

    return List.of(p1, p11, p12, n1, n2, n3, n4);
  }

  private static void deleteTree(List<Vertex> vs) {
    final List<Vertex> mvs = new ArrayList<>(vs);
    // change order of vertices before deleting
    Collections.shuffle(mvs);
    mvs.forEach((v) -> {
      v.delete();
    });
  }
}
