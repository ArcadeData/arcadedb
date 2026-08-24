package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Standalone reproduction of the exact scenario reported in https://github.com/ArcadeData/arcadedb/issues/6667:
 * a vertex type rename must not corrupt the type's edge-chunk bucket names, or an edge insert on the renamed
 * type afterward fails with SchemaException. Kept separate from {@link TypeRenameComponentNamingTest} because it
 * mirrors the issue's own reproducer verbatim.
 */
class Issue6667ReproTest {
  private static final String DB_PATH = "target/databases/issue6667repro";

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void renamedVertexTypeCanStillInsertEdges() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    try (DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (Database db = factory.create()) {
        db.getSchema().createVertexType("Person");
        db.getSchema().createEdgeType("Knows");

        db.transaction(() -> {
          MutableVertex v1 = db.newVertex("Person").set("uid", "a").save();
          MutableVertex v2 = db.newVertex("Person").set("uid", "b").save();
          v1.newEdge("Knows", v2).save();
        });

        db.getSchema().getType("Person").rename("Human");

        db.transaction(() -> {
          MutableVertex v3 = db.newVertex("Human").set("uid", "c").save();
          MutableVertex v4 = db.newVertex("Human").set("uid", "d").save();
          v3.newEdge("Knows", v4).save();
        });

        assertThat(db.countType("Human", true)).isEqualTo(4L);
      }
    }
  }
}
