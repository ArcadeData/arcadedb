package com.arcadedb.database;

import org.junit.jupiter.api.Test;

public class MigrationManualTest {

  @Test
  public void testOpenV0Database() {
    String dbPath = "/Users/luca/Documents/GitHub/arcadedb/server/databases/stackoverflow_tiny_graph_olap_arcadedb";

    System.out.println("Opening database: " + dbPath);

    try {
      DatabaseFactory factory = new DatabaseFactory(dbPath);
      Database db = factory.open();

      System.out.println("Database opened successfully!");
      System.out.println("Schema types: " + db.getSchema().getTypes().size());

      // Test vertex count
      long vertexCount = db.countType("User", true);
      System.out.println("User vertices: " + vertexCount);

      // Test edge operations on a vertex
      db.begin();
      try {
        var result = db.query("sql", "SELECT FROM User LIMIT 1");
        if (result.hasNext()) {
          var record = result.next().toElement();
          System.out.println("Sample user: " + record.toJSON());

          if (record.asVertex() != null) {
            var vertex = record.asVertex();
            long outEdges = vertex.countEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, null);
            long inEdges = vertex.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, null);
            System.out.println("Vertex edges - out: " + outEdges + ", in: " + inEdges);
          }
        }
        db.commit();
      } catch (Exception e) {
        db.rollback();
        throw e;
      }

      db.close();
      System.out.println("Database closed successfully!");

    } catch (Exception e) {
      System.err.println("Error opening database:");
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
