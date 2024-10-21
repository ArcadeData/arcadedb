package com.arcadedb.engine;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteAllTest {
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

        for (int i = 0; i < CYCLES; i++) {
          //System.out.println("Cycle " + i);
//          List.of(new File(databaseFactory.getDatabasePath()).listFiles())
//              .forEach(f -> System.out.println("- " + f.getName() + ": " + FileUtils.getSizeAsString(
//                  f.length())));

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

          db.transaction(() -> {
            assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(TOT_RECORDS);
            assertThat(db.countType(EDGE_TYPE, true)).isEqualTo(TOT_RECORDS - 1);

            db.command("sql", "delete from " + VERTEX_TYPE);

            assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(0);
            assertThat(db.countType(EDGE_TYPE, true)).isEqualTo(0);
          });
        }

        final ResultSet result = db.command("sql", "check database");
//        System.out.println(result.nextIfAvailable().toJSON());
      } finally {

        if (databaseFactory.exists())
          databaseFactory.open().drop();
      }
    }
  }
}
