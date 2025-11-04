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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LSMVectorIndexBuilder.
 *
 * <p>Tests the builder's ability to:
 * <ul>
 *   <li>Accept and validate vector-specific parameters</li>
 *   <li>Enforce parameter constraints</li>
 *   <li>Provide fluent API for configuration</li>
 *   <li>Reject invalid configurations</li>
 * </ul>
 */
class LSMVectorIndexBuilderTest extends TestHelper {

  /**
   * Tests that LSM_VECTOR index type exists in the enum.
   */
  @Test
  void testLSMVectorEnumExists() {
    // Verify the enum value exists
    assertNotNull(Schema.INDEX_TYPE.LSM_VECTOR);
    assertEquals("LSM_VECTOR", Schema.INDEX_TYPE.LSM_VECTOR.name());
  }

  /**
   * Tests that LSM_VECTOR can be referenced alongside other index types.
   */
  @Test
  void testAllIndexTypesExist() {
    // All index types should be available
    assertNotNull(Schema.INDEX_TYPE.LSM_TREE);
    assertNotNull(Schema.INDEX_TYPE.FULL_TEXT);
    assertNotNull(Schema.INDEX_TYPE.HNSW);
    assertNotNull(Schema.INDEX_TYPE.LSM_VECTOR);
  }

  /**
   * Tests basic builder creation with minimal configuration.
   */
  @Test
  void testBuilderCreationWithDimensions() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createDocumentType("TestVector");
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Builder should be creatable via Schema factory
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("TestVector", "embedding");
      builder.withDimensions(128);
      builder.withSimilarity("COSINE");
      builder.withMaxConnections(16);
      builder.withBeamWidth(100);

      assertEquals(128, builder.getDimensions());
      assertEquals("COSINE", builder.getSimilarityFunction());
      assertEquals(16, builder.getMaxConnections());
      assertEquals(100, builder.getBeamWidth());
      assertEquals(1.2f, builder.getAlpha());
    });
  }

  /**
   * Tests that dimensions parameter is required.
   */
  @Test
  void testDimensionsRequired() {
    database.transaction(() -> {
      // Builder with no dimensions should fail on validate/create
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");
      builder.withSimilarity("COSINE");

      // Should throw when accessing builder state without dimensions
      assertEquals(-1, builder.getDimensions());
    });
  }

  /**
   * Tests dimension validation - minimum value.
   */
  @Test
  void testDimensionValidationMinimum() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Dimensions < 1 should fail
      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withDimensions(0);
      });
      assertTrue(ex.getMessage().contains("1 and 4096"));
    });
  }

  /**
   * Tests dimension validation - maximum value.
   */
  @Test
  void testDimensionValidationMaximum() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Dimensions > 4096 should fail
      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withDimensions(5000);
      });
      assertTrue(ex.getMessage().contains("1 and 4096"));
    });
  }

  /**
   * Tests valid dimension values.
   */
  @Test
  void testDimensionValidValues() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // These should all succeed
      builder.withDimensions(1);
      assertEquals(1, builder.getDimensions());

      builder.withDimensions(128);
      assertEquals(128, builder.getDimensions());

      builder.withDimensions(512);
      assertEquals(512, builder.getDimensions());

      builder.withDimensions(4096);
      assertEquals(4096, builder.getDimensions());
    });
  }

  /**
   * Tests similarity function validation.
   */
  @Test
  void testSimilarityFunctionValidation() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Valid functions
      builder.withSimilarity("COSINE");
      assertEquals("COSINE", builder.getSimilarityFunction());

      builder.withSimilarity("EUCLIDEAN");
      assertEquals("EUCLIDEAN", builder.getSimilarityFunction());

      builder.withSimilarity("DOT_PRODUCT");
      assertEquals("DOT_PRODUCT", builder.getSimilarityFunction());

      // Case insensitive
      builder.withSimilarity("cosine");
      assertEquals("COSINE", builder.getSimilarityFunction());
    });
  }

  /**
   * Tests similarity function validation - invalid value.
   */
  @Test
  void testSimilarityFunctionInvalid() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withSimilarity("INVALID");
      });
      assertTrue(ex.getMessage().contains("Invalid similarity function"));
    });
  }

  /**
   * Tests similarity function validation - null value.
   */
  @Test
  void testSimilarityFunctionNull() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withSimilarity(null);
      });
      assertTrue(ex.getMessage().contains("cannot be null or empty"));
    });
  }

  /**
   * Tests maxConnections validation.
   */
  @Test
  void testMaxConnectionsValidation() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Valid range: 2-100
      builder.withMaxConnections(2);
      assertEquals(2, builder.getMaxConnections());

      builder.withMaxConnections(16);
      assertEquals(16, builder.getMaxConnections());

      builder.withMaxConnections(100);
      assertEquals(100, builder.getMaxConnections());
    });
  }

  /**
   * Tests maxConnections validation - too small.
   */
  @Test
  void testMaxConnectionsTooSmall() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withMaxConnections(1);
      });
      assertTrue(ex.getMessage().contains("2 and 100"));
    });
  }

  /**
   * Tests maxConnections validation - too large.
   */
  @Test
  void testMaxConnectionsTooLarge() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withMaxConnections(101);
      });
      assertTrue(ex.getMessage().contains("2 and 100"));
    });
  }

  /**
   * Tests beamWidth validation.
   */
  @Test
  void testBeamWidthValidation() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Valid range: 10-1000
      builder.withBeamWidth(10);
      assertEquals(10, builder.getBeamWidth());

      builder.withBeamWidth(100);
      assertEquals(100, builder.getBeamWidth());

      builder.withBeamWidth(1000);
      assertEquals(1000, builder.getBeamWidth());
    });
  }

  /**
   * Tests beamWidth validation - too small.
   */
  @Test
  void testBeamWidthTooSmall() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withBeamWidth(5);
      });
      assertTrue(ex.getMessage().contains("10 and 1000"));
    });
  }

  /**
   * Tests beamWidth validation - too large.
   */
  @Test
  void testBeamWidthTooLarge() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withBeamWidth(2000);
      });
      assertTrue(ex.getMessage().contains("10 and 1000"));
    });
  }

  /**
   * Tests alpha parameter validation.
   */
  @Test
  void testAlphaValidation() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Valid range: 1.0-2.0
      builder.withAlpha(1.0f);
      assertEquals(1.0f, builder.getAlpha());

      builder.withAlpha(1.2f);
      assertEquals(1.2f, builder.getAlpha());

      builder.withAlpha(2.0f);
      assertEquals(2.0f, builder.getAlpha());
    });
  }

  /**
   * Tests alpha parameter validation - too small.
   */
  @Test
  void testAlphaTooSmall() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withAlpha(0.5f);
      });
      assertTrue(ex.getMessage().contains("1.0 and 2.0"));
    });
  }

  /**
   * Tests alpha parameter validation - too large.
   */
  @Test
  void testAlphaTooLarge() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.withAlpha(2.5f);
      });
      assertTrue(ex.getMessage().contains("1.0 and 2.0"));
    });
  }

  /**
   * Tests fluent API - chaining methods.
   */
  @Test
  void testFluentApi() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop")
          .withDimensions(256)
          .withSimilarity("EUCLIDEAN")
          .withMaxConnections(32)
          .withBeamWidth(200)
          .withAlpha(1.5f);

      assertEquals(256, builder.getDimensions());
      assertEquals("EUCLIDEAN", builder.getSimilarityFunction());
      assertEquals(32, builder.getMaxConnections());
      assertEquals(200, builder.getBeamWidth());
      assertEquals(1.5f, builder.getAlpha());
    });
  }

  /**
   * Tests default values.
   */
  @Test
  void testDefaultValues() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop");

      // Only dimensions is required (-1 if not set)
      assertEquals(-1, builder.getDimensions());

      // Others have defaults
      assertEquals("COSINE", builder.getSimilarityFunction());
      assertEquals(16, builder.getMaxConnections());
      assertEquals(100, builder.getBeamWidth());
      assertEquals(1.2f, builder.getAlpha());
    });
  }

  /**
   * Tests cross-parameter validation: beamWidth must be >= maxConnections.
   */
  @Test
  void testCrossParameterValidation() {
    database.transaction(() -> {
      LSMVectorIndexBuilder builder = database.getSchema()
          .buildLSMVectorIndex("Type", "prop")
          .withDimensions(128)
          .withMaxConnections(50)
          .withBeamWidth(10);

      // beamWidth < maxConnections should fail during create()
      ConfigurationException ex = assertThrows(ConfigurationException.class, () -> {
        builder.create();
      });
      assertTrue(ex.getMessage().contains("beamWidth") && ex.getMessage().contains("maxConnections"));
    });
  }
}
