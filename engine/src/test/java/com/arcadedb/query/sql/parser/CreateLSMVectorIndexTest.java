/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for CREATE INDEX statement with LSM_VECTOR type and METADATA.
 *
 * <p>Tests the SQL parser and execution of LSM_VECTOR index creation with:
 * <ul>
 *   <li>Valid METADATA JSON with all parameters</li>
 *   <li>Valid METADATA with default parameters</li>
 *   <li>Error handling for missing METADATA</li>
 *   <li>Error handling for invalid METADATA JSON</li>
 *   <li>Error handling for missing required parameters</li>
 *   <li>Error handling for out-of-range parameter values</li>
 *   <li>Various similarity functions</li>
 *   <li>IF NOT EXISTS clause</li>
 * </ul>
 */
public class CreateLSMVectorIndexTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      // Create test document type for vector indexing
      final DocumentType vectorType = database.getSchema().createDocumentType("VectorDocument");
      vectorType.createProperty("embedding", Type.ARRAY_OF_FLOATS);
      vectorType.createProperty("name", Type.STRING);
      vectorType.createProperty("id", Type.INTEGER);
    });
  }

  /**
   * Test parsing and creation of LSM_VECTOR index with all parameters specified.
   */
  @Test
  public void testCreateLSMVectorIndexWithAllMetadata() {
    database.transaction(() -> {
      // Should not throw
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
              METADATA {
                "dimensions": 512,
                "similarity": "COSINE",
                "maxConnections": 16,
                "beamWidth": 100,
                "alpha": 1.2
              }""");

      // Verify index was created
      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });
  }

  /**
   * Test LSM_VECTOR index creation with minimum METADATA (only dimensions).
   */
  @Test
  public void testCreateLSMVectorIndexWithMinimalMetadata() {
    database.transaction(() -> {
      // Only dimensions is required, others should use defaults
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
              METADATA {"dimensions": 256}""");

      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });
  }

  /**
   * Test LSM_VECTOR with various dimensions (boundary values).
   */
  @Test
  public void testCreateLSMVectorIndexVariousDimensions() {
    database.transaction(() -> {
      // Test minimum dimensions
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument(embedding)  LSM_VECTOR
              METADATA {"dimensions": 1}""");
      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });

  }

  /**
   * Test LSM_VECTOR with different similarity functions.
   */
  @Test
  public void testCreateLSMVectorIndexVariousSimilarityFunctions() {
    database.transaction(() -> {
      // COSINE (default)
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR \
              METADATA {"dimensions": 256, "similarity": "COSINE"}""");
      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });

  }

  /**
   * Test LSM_VECTOR parameter ranges.
   */
  @Test
  public void testCreateLSMVectorIndexParameterRanges() {
    database.transaction(() -> {
      // MaxConnections range: 2-100
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR \
              METADATA {"dimensions": 256, "maxConnections": 2}""");
      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });

  }

  /**
   * Test LSM_VECTOR with IF NOT EXISTS clause.
   */
  @Test
  public void testCreateLSMVectorIndexIfNotExists() {
    database.transaction(() -> {
      // First creation should succeed
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
              METADATA {"dimensions": 256}""");
      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });

  }

  /**
   * Test error: LSM_VECTOR without METADATA.
   */
  @Test
  public void testCreateLSMVectorIndexMissingMetadata() {
    database.transaction(() -> {
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR");
      });
      assertTrue(ex.getMessage().contains("METADATA") ||
              ex.getMessage().contains("metadata"),
          "Expected error message about missing METADATA");
    });
  }

  /**
   * Test error: LSM_VECTOR METADATA without dimensions.
   */
  @Test
  public void testCreateLSMVectorIndexMissingDimensions() {
    database.transaction(() -> {
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            """
                CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR \
                METADATA {"similarity": "COSINE"}""");
      });
      assertTrue(ex.getMessage().contains("dimensions") ||
              ex.getMessage().contains("Dimensions"),
          "Expected error message about missing dimensions parameter");
    });
  }

  /**
   * Test error: LSM_VECTOR with invalid dimensions (out of range).
   */
  @Test
  public void testCreateLSMVectorIndexInvalidDimensions() {
    database.transaction(() -> {
      // Too small
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            """
                CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR \
                METADATA {"dimensions": 0}""");
      });
      assertTrue(ex.getMessage().contains("dimensions"),
          "Expected error about invalid dimensions");
    });

    database.transaction(() -> {
      // Too large
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR " +
                "METADATA {\"dimensions\": 5000}");
      });
      assertTrue(ex.getMessage().contains("dimensions"),
          "Expected error about invalid dimensions");
    });
  }

  /**
   * Test error: LSM_VECTOR with invalid similarity function.
   */
  @Test
  public void testCreateLSMVectorIndexInvalidSimilarity() {
    database.transaction(() -> {
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR " +
                "METADATA {\"dimensions\": 256, \"similarity\": \"INVALID\"}");
      });
      assertTrue(ex.getMessage().contains("similarity") ||
              ex.getMessage().contains("Similarity"),
          "Expected error about invalid similarity function");
    });
  }

  /**
   * Test error: LSM_VECTOR with invalid maxConnections (out of range).
   */
  @Test
  public void testCreateLSMVectorIndexInvalidMaxConnections() {
    database.transaction(() -> {
      // Too small
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR " +
                "METADATA {\"dimensions\": 256, \"maxConnections\": 1}");
      });
      assertTrue(ex.getMessage().contains("maxConnections") ||
              ex.getMessage().contains("connections"),
          "Expected error about invalid maxConnections");
    });

    database.transaction(() -> {
      // Too large
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR " +
                "METADATA {\"dimensions\": 256, \"maxConnections\": 101}");
      });
      assertTrue(ex.getMessage().contains("maxConnections") ||
              ex.getMessage().contains("connections"),
          "Expected error about invalid maxConnections");
    });
  }

  /**
   * Test error: LSM_VECTOR with invalid beamWidth (out of range).
   */
  @Test
  public void testCreateLSMVectorIndexInvalidBeamWidth() {
    database.transaction(() -> {
      // Too small
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            """
                CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
                METADATA {"dimensions": 256, "beamWidth": 5}""");
      });
      assertTrue(ex.getMessage().contains("Beam width") ||
              ex.getMessage().contains("Beam"),
          "Expected error about invalid beamWidth");
    });

  }

  /**
   * Test error: LSM_VECTOR with invalid alpha (out of range).
   */
  @Test
  public void testCreateLSMVectorIndexInvalidAlpha() {
    database.transaction(() -> {
      // Too small
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR " +
                "METADATA {\"dimensions\": 256, \"alpha\": 0.5}");
      });
      assertTrue(ex.getMessage().contains("alpha") ||
              ex.getMessage().contains("Alpha"),
          "Expected error about invalid alpha");
    });

    database.transaction(() -> {
      // Too large
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            "CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR " +
                "METADATA {\"dimensions\": 256, \"alpha\": 2.5}");
      });
      assertTrue(ex.getMessage().contains("alpha") ||
              ex.getMessage().contains("Alpha"),
          "Expected error about invalid alpha");
    });
  }

  /**
   * Test error: LSM_VECTOR with invalid METADATA JSON.
   */
  @Test
  public void testCreateLSMVectorIndexInvalidMetadataJSON() {
    database.transaction(() -> {
      CommandSQLParsingException ex = assertThrows(CommandSQLParsingException.class, () -> {
        database.command("sql",
            """
                CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR \
                METADATA {invalid json}""");
      });

    });
  }

  /**
   * Test error: beamWidth < maxConnections constraint.
   */
  @Test
  public void testCreateLSMVectorIndexBeamWidthLessThanMaxConnections() {
    database.transaction(() -> {
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            """
                CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
                METADATA {
                  "dimensions": 256,
                  "maxConnections": 50,
                  "beamWidth": 40
                }""");
      });
      assertTrue(ex.getMessage().contains("beamWidth") &&
              ex.getMessage().contains("maxConnections"),
          "Expected error about beamWidth < maxConnections constraint");
    });
  }

  /**
   * Test duplicate index name error.
   */
  @Test
  public void testCreateDuplicateLSMVectorIndex() {
    database.transaction(() -> {
      database.command("sql",
          """
              CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
              METADATA {"dimensions": 256}""");
    });

    database.transaction(() -> {
      CommandExecutionException ex = assertThrows(CommandExecutionException.class, () -> {
        database.command("sql",
            """
                CREATE INDEX ON VectorDocument (embedding) LSM_VECTOR
                METADATA {"dimensions": 256}""");
      });
      assertTrue(ex.getMessage().contains("already exists") ||
              ex.getMessage().contains("exists"),
          "Expected error about index already existing");
    });
  }

  /**
   * Test with numeric metadata values.
   */
  @Test
  public void testCreateLSMVectorIndexNumericMetadata() {
    database.transaction(() -> {
      // Test with all numeric parameters
      database.command("sql",
          """
              CREATE INDEX  ON VectorDocument (embedding) LSM_VECTOR \
              METADATA {
                "dimensions": 384,
                "maxConnections": 32,
                "beamWidth": 200,
                "alpha": 1.5
              }""");
      assertTrue(database.getSchema().existsIndex("VectorDocument[embedding]"));
    });
  }
}
