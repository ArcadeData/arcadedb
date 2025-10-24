package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;

public class JvectorExample {

  public static void main(String[] args) {
    DatabaseFactory factory = new DatabaseFactory("./target/databases/vector_example");

    Database database = factory.create();

    database.transaction(() -> {
      database.command("sqlscript", """
          CREATE VERTEX TYPE VectorDocument IF NOT EXISTS;
          CREATE PROPERTY VectorDocument.id IF NOT EXISTS STRING;
          CREATE PROPERTY VectorDocument.embedding IF NOT EXISTS ARRAY_OF_FLOATS;

          CREATE INDEX IF NOT EXISTS ON VectorDocument (id) UNIQUE;

          CREATE INDEX IF NOT EXISTS ON VectorDocument (embedding) JVECTOR
            METADATA {
              "dimensions" : 4,
              "similarity" : "COSINE",
              "maxConnections" : 16,
              "beamWidth" : 100,
              "enableDiskPersistence": true,
              "diskPersistenceThreshold": 100,
              "memoryLimitMB": 10
              };
          """);
    });

    database.transaction(() ->
        database.command("sqlscript", """
            INSERT INTO VectorDocument CONTENT {
              "id": "doc1",
              "title": "Technology Article",
              "embedding": [0.1, 0.2, 0.3, 0.4]
            };
            INSERT INTO VectorDocument CONTENT {
              "id": "doc2",
              "title": "Science Research",
              "embedding": [0.2, 0.3, 0.4, 0.5]
            };
            INSERT INTO VectorDocument CONTENT {
              "id": "doc3",
              "title": "Business News",
              "embedding": [0.8, 0.1, 0.2, 0.1]
            };
            INSERT INTO VectorDocument CONTENT {
              "id": "doc4",
              "title": "Sports Update",
              "embedding": [0.0, 0.0, 1.0, 0.0]
            };
            """)
    );

    // Step 3: Test SQL vectorNeighbors function with array literal syntax
    ResultSet resultSet = database.query("SQL",
        "SELECT vectorNeighbors('VectorDocument[embedding]', [1.0, 0.0, 0.0, 0.0], 3) as neighbors");

    resultSet.stream().forEach(r-> System.out.println("r.toJSON() = " + r.toJSON()));

    database.close();
  }
}
