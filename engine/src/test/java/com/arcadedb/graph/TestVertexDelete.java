package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class TestVertexDelete {
  public static class DeleteOnClose implements AutoCloseable {
    public DeleteOnClose(Path p) {
      this.p = p;
    }

    public Path getPath() {
      return p;
    }

    public void close() {
      deleteDirectory(p.toFile());
    }

    private void deleteDirectory(File directory) {
      if (directory.exists()) {
        File[] files = directory.listFiles();
        if (files != null) {
          for (File file : files) {
            if (file.isDirectory()) {
              deleteDirectory(file);
            } else {
              if (!file.delete())
                System.out.println("Could not delete file " + file);
            }
          }
        }
        if (!directory.delete())
          System.out.println("Could not delete directory " + directory);
      }
    }

    private final Path p;
  }

  @Test
  public void testFullEdgeDeletion() throws IOException, InterruptedException {
    try (var td = createTemporaryDirectory(); var df = new DatabaseFactory(td.getPath().toString()); var db = df.create()) {
      createSchema(db);
      for (int i = 0; i < 100; i++) {
        List<Vertex> vlist = new ArrayList<>();
        db.transaction(() -> {
          vlist.addAll(createTree(db));
          deleteTree(vlist);
        });

        db.transaction(() -> {
          var v1c = db.countType("v1", false);
          var pc = db.countType("hasParent", false);
          Assertions.assertEquals(0, v1c);
          Assertions.assertEquals(0, pc);
        });
      }
    }
  }

  private static DeleteOnClose createTemporaryDirectory() throws IOException {
    return new DeleteOnClose(Files.createTempDirectory("arcadedb"));
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
    p11.newEdge("hasParent", p1, true).save();
    var p12 = db.newVertex("v1").save();
    p12.newEdge("hasParent", p1, true).save();
    var n1 = db.newVertex("v1").save();
    n1.newEdge("hasParent", p1, true).save();
    var n2 = db.newVertex("v1").save();
    n2.newEdge("hasParent", p11, true).save();
    var n3 = db.newVertex("v1").save();
    n3.newEdge("hasParent", p11, true).save();
    var n4 = db.newVertex("v1").save();
    n4.newEdge("hasParent", p12, true).save();

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
