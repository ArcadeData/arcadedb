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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test cases for Database.getSize() API
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseGetSizeTest extends TestHelper {

  @Test
  void getSizeOnEmptyDatabase() {
    // Even an empty database has metadata files (schema, configuration, etc.)
    final long initialSize = database.getSize();
    assertThat(initialSize).isGreaterThan(0L);
  }

  @Test
  void getSizeIncreasesWithDocuments() {
    database.transaction(() -> {
      // Create a document type
      final DocumentType personType = database.getSchema().createDocumentType("Person");
      personType.createProperty("name", Type.STRING);
      personType.createProperty("age", Type.INTEGER);
      personType.createProperty("email", Type.STRING);
    });

    final long sizeBeforeInsert = database.getSize();

    // Insert documents
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        final MutableDocument doc = database.newDocument("Person");
        doc.set("name", "Person" + i);
        doc.set("age", 20 + (i % 50));
        doc.set("email", "person" + i + "@example.com");
        doc.save();
      }
    });

    final long sizeAfterInsert = database.getSize();

    // Database size should increase after inserting documents
    assertThat(sizeAfterInsert).isGreaterThan(sizeBeforeInsert);
  }

  @Test
  void getSizeIncreasesWithVerticesAndEdges() {
    database.transaction(() -> {
      // Create vertex and edge types
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("Follows");
    });

    final long sizeBeforeData = database.getSize();

    // Create vertices and edges
    database.transaction(() -> {
      MutableVertex user1 = database.newVertex("User");
      user1.set("name", "Alice");
      user1.save();

      MutableVertex user2 = database.newVertex("User");
      user2.set("name", "Bob");
      user2.save();

      MutableVertex user3 = database.newVertex("User");
      user3.set("name", "Charlie");
      user3.save();

      // Create edges (using explicit method to avoid ambiguity)
      user1.newEdge("Follows", user2, true, (Object[]) null);
      user2.newEdge("Follows", user3, true, (Object[]) null);
      user1.newEdge("Follows", user3, true, (Object[]) null);
    });

    final long sizeAfterData = database.getSize();

    // Database size should increase after inserting vertices and edges
    assertThat(sizeAfterData).isGreaterThan(sizeBeforeData);
  }

  @Test
  void getSizeWithIndex() {
    database.transaction(() -> {
      // Create a document type with an indexed property
      final DocumentType productType = database.getSchema().createDocumentType("Product");
      productType.createProperty("sku", Type.STRING);
      productType.createProperty("name", Type.STRING);
      productType.createProperty("price", Type.DOUBLE);

      // Create an index
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Product", "sku");
    });

    final long sizeBeforeInsert = database.getSize();

    // Insert documents that will be indexed
    database.transaction(() -> {
      for (int i = 0; i < 500; i++) {
        final MutableDocument doc = database.newDocument("Product");
        doc.set("sku", "SKU-" + String.format("%05d", i));
        doc.set("name", "Product " + i);
        doc.set("price", 10.0 + i);
        doc.save();
      }
    });

    final long sizeAfterInsert = database.getSize();

    // Database size should increase (includes both data and index files)
    assertThat(sizeAfterInsert).isGreaterThan(sizeBeforeInsert);
  }

  @Test
  void getSizeMultipleCalls() {
    // Multiple calls to getSize() should return consistent results
    final long size1 = database.getSize();
    final long size2 = database.getSize();
    final long size3 = database.getSize();

    assertThat(size1).isEqualTo(size2);
    assertThat(size2).isEqualTo(size3);
  }

  @Test
  void getSizeAfterDelete() {
    database.transaction(() ->
      database.getSchema().createDocumentType("TempDoc"));

    // Insert documents
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument doc = database.newDocument("TempDoc");
        doc.set("value", "Data" + i);
        doc.save();
      }
    });

    final long sizeAfterInsert = database.getSize();

    // Delete some documents
    database.transaction(() ->
      database.command("sql", "DELETE FROM TempDoc WHERE value LIKE 'Data5%'").close());

    final long sizeAfterDelete = database.getSize();

    // Size might not decrease immediately due to space not being reclaimed until compaction
    // But we can verify the size is still valid
    assertThat(sizeAfterDelete).isGreaterThan(0L);
  }

  /**
   * Regression test for issue #4576: when getSize() fails, the original exception must be preserved
   * as the cause of the DatabaseOperationException, instead of {@code e.getCause()} which is null (or
   * a different, inner exception) and loses the real stack trace.
   * <p>
   * An unreadable subdirectory makes {@link java.nio.file.Files#walk} throw an
   * {@link UncheckedIOException}: the buggy code rewrapped it with {@code e.getCause()} (the inner
   * AccessDeniedException, or null), while the fix propagates the actual exception {@code e}.
   */
  @Test
  @DisabledOnOs(OS.WINDOWS)
  void getSizePreservesOriginalCauseOnFailure() {
    final Path unreadable = Path.of(database.getDatabasePath(), "unreadable-4576");
    try {
      Files.createDirectory(unreadable);
      // Strip every permission so Files.walk cannot descend into the directory.
      Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("---------"));

      // Skip when the permission denial has no effect (e.g. running as root): there is nothing to assert.
      try (Stream<Path> stream = Files.walk(Path.of(database.getDatabasePath()))) {
        stream.forEach(p -> {
        });
        assumeTrue(false, "Filesystem walk is not blocked by permissions (likely running as root); skipping");
      } catch (UncheckedIOException expected) {
        // good: traversal is blocked, getSize() will hit the catch block
      }

      assertThatThrownBy(() -> database.getSize())
          .isInstanceOf(DatabaseOperationException.class)
          // The fix passes the original exception (UncheckedIOException) as the cause; the buggy code
          // passed e.getCause(), losing the wrapper (and being null for non-IOException failures).
          .hasCauseInstanceOf(UncheckedIOException.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // Restore permissions so TestHelper can clean up the database directory.
      try {
        if (Files.exists(unreadable)) {
          Files.setPosixFilePermissions(unreadable, PosixFilePermissions.fromString("rwxr-xr-x"));
          Files.delete(unreadable);
        }
      } catch (Exception ignore) {
        // best effort
      }
    }
  }
}
