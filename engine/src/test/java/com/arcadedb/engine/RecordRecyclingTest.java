package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordRecyclingTest {
  private final static int    TOT_RECORDS = 100_000;
  private final static String VERTEX_TYPE = "Product";
  private final static String EDGE_TYPE   = "LinkedTo";
  private static final int    CYCLES      = 2;

  @Test
  public void testCreateAndDeleteGraph() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/DeleteAllTest")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
        db.getSchema().createVertexType(VERTEX_TYPE, 1);
        db.getSchema().createEdgeType(EDGE_TYPE, 1);

        RID maxVertexRID = null;
        RID maxEdgeRID = null;

        for (int i = 0; i < CYCLES; i++) {
          db.transaction(() -> {
            final MutableVertex root = db.newVertex(VERTEX_TYPE)//
                .set("id", 0)//
                .save();

            for (int k = 1; k < TOT_RECORDS; k++) {
              final MutableVertex v = db.newVertex(VERTEX_TYPE)//
                  .set("id", k)//
                  .save();

              root.newEdge(EDGE_TYPE, v, true, "something", k);
            }
          });

          // CHECK RECORDS HAVE BEEN RECYCLED MORE THAN 80% OF THE SPACE
          final RID maxVertex = db.query("sql", "select from " + VERTEX_TYPE + " order by @rid desc").next().getIdentity().get();
          if (maxVertexRID != null)
            assertThat(maxVertex.getPosition()).isLessThan((long) (maxVertexRID.getPosition() * 1.2));
          maxVertexRID = maxVertex;

          final RID maxEdge = db.query("sql", "select from " + EDGE_TYPE + " order by @rid desc").next().getIdentity().get();
          if (maxEdgeRID != null)
            assertThat(maxEdge.getPosition()).isLessThan((long) (maxEdgeRID.getPosition() * 1.2));
          maxEdgeRID = maxEdge;

          db.transaction(() -> {
            assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(TOT_RECORDS);
            assertThat(db.countType(EDGE_TYPE, true)).isEqualTo(TOT_RECORDS - 1);

            db.command("sql", "delete from " + VERTEX_TYPE);

            assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(0);
            assertThat(db.countType(EDGE_TYPE, true)).isEqualTo(0);

            assertThat(db.countBucket(db.getSchema().getBucketById(1).getName())).isEqualTo(0);
            assertThat(db.countBucket(db.getSchema().getBucketById(2).getName())).isEqualTo(0);
            assertThat(db.countBucket(db.getSchema().getBucketById(3).getName())).isEqualTo(0);
            assertThat(db.countBucket(db.getSchema().getBucketById(4).getName())).isEqualTo(0);
          });
        }

        final ResultSet result = db.command("sql", "check database");
        // System.out.println(result.nextIfAvailable().toJSON());
      } finally {

        if (databaseFactory.exists())
          databaseFactory.open().drop();
      }
    }
  }
}
