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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6762: a failed first-use materialisation must not leave the vector-id allocator at 0.
 * <p>
 * {@code materializeLocations()} catches a load failure, logs a WARNING and still marks the materialisation done -
 * deliberately, because the alternative is re-parsing a corrupt index once per query instead of once per open. But
 * {@code nextId} was only advanced at the END of a successful load, so a failure that threw after some entries had
 * already been inserted left the map populated and the allocator at 0. {@code allocateVectorId()} exists precisely
 * to stop ids restarting at 0 on top of live entries, and it would then hand out 0, 1, 2... - so the next insert
 * superseded a live vector instead of adding one, losing one vector per insert.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6762VectorIdAllocatorAfterFailedLoadTest {
  private static final String DB_PATH    = "target/databases/Issue6762VectorIdAllocatorAfterFailedLoadTest";
  private static final int    DIMENSIONS = 8;
  private static final int    VECTORS    = 20;

  @AfterEach
  void cleanUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void anInsertAfterAFailedMaterialisationDoesNotSupersedeALiveVector() throws Exception {
    createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);

        // Populate the location map for real, then put the index back into the state a PARTIAL load leaves behind:
        // entries already in the map, the one-attempt flag not yet latched, and the allocator still at its initial
        // 0. Breaking the mutable component makes the materialisation that follows fail the way the real one does.
        assertThat(index.countEntries()).isEqualTo(VECTORS);
        final Object mutable = readField(index, "mutable");
        setField(index, "locationsMaterialized", false);
        ((AtomicInteger) readField(index, "nextId")).set(0);
        setField(index, "mutable", null);
        try {
          index.getVectorIndex(); // materialises: throws internally, and the failure is swallowed by design
        } finally {
          setField(index, "mutable", mutable);
        }

        assertThat(index.areLocationsMaterializedForTest())
            .as("the one-attempt contract still holds: a failed load is not retried")
            .isTrue();
        assertThat(((AtomicInteger) readField(index, "nextId")).get())
            .as("but the allocator must sit past every id the map already holds, not back at 0")
            .isGreaterThanOrEqualTo(VECTORS);

        // The observable consequence: with the allocator at 0 this insert would overwrite the entry holding id 0.
        db.transaction(() -> db.newDocument("Doc").set("id", VECTORS).set("embedding", randomVector(7)).save());

        assertThat(index.countEntries())
            .as("an insert must ADD a vector, not supersede a live one")
            .isEqualTo(VECTORS + 1);
      } finally {
        db.close();
      }
    }
  }

  private LSMVectorIndex vectorIndexOf(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final long seed) {
    final Random rnd = new Random(seed);
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) rnd.nextGaussian();
    return v;
  }

  private void createDatabaseWithVectorIndex() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          for (int i = 0; i < VECTORS; i++)
            db.newDocument("Doc").set("id", i).set("embedding", randomVector(i)).save();
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\" }");
      } finally {
        db.close();
      }
    }
  }

  private static Object readField(final Object target, final String name) throws Exception {
    final Field f = LSMVectorIndex.class.getDeclaredField(name);
    f.setAccessible(true);
    return f.get(target);
  }

  private static void setField(final Object target, final String name, final Object value) throws Exception {
    final Field f = LSMVectorIndex.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
