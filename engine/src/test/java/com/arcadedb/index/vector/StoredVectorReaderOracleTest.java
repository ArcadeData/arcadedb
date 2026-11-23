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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The vector a search reads back from the page of its record, without loading the record: for documents and vertices, with the
 * vector property among other properties of every kind in a random order, the answer is the vector that was stored, read in place
 * (no fall back to the record layer); a record without the property, with a null, with a vector of another length, or whose vector
 * is all zeros when zero is unscorable, has no vector.
 */
class StoredVectorReaderOracleTest extends TestHelper {
  private static final int DIM = 7;

  private record Expected(RID rid, float[] vector, boolean zeroVector) {
  }

  @ParameterizedTest
  @ValueSource(strings = { "DOCUMENT", "VERTEX" })
  void theVectorOfARecordIsReadInPlaceWhateverItsOtherProperties(final String kind) {
    final String create = kind.equals("VERTEX") ? "CREATE VERTEX TYPE Rec" : "CREATE DOCUMENT TYPE Rec";
    database.transaction(() -> {
      database.command("sql", create);
      database.command("sql", "CREATE PROPERTY Rec.vector ARRAY_OF_FLOATS");
    });
    final Random rnd = new Random(88);
    final List<Expected> present = new ArrayList<>();
    final List<RID> absent = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 120; i++) {
        final MutableDocument d = kind.equals("VERTEX") ? database.newVertex("Rec") : database.newDocument("Rec");
        final int variant = rnd.nextInt(8);
        // the other properties and the vector, in a random order of assignment
        final List<Runnable> sets = new ArrayList<>();
        final int extras = rnd.nextInt(7);
        for (int e = 0; e < extras; e++) {
          final int shape = rnd.nextInt(7);
          final String name = "p" + e;
          sets.add(() -> {
            switch (shape) {
            case 0 -> d.set(name, "s".repeat(rnd.nextInt(300)));
            case 1 -> d.set(name, rnd.nextInt());
            case 2 -> d.set(name, rnd.nextLong());
            case 3 -> d.set(name, rnd.nextDouble());
            case 4 -> d.set(name, rnd.nextBoolean());
            case 5 -> d.set(name, List.of(1, 2, rnd.nextInt(50)));
            default -> d.set(name, new HashMap<>(Map.of("k", rnd.nextInt(9))));
            }
          });
        }
        float[] vector = null;
        boolean zero = false;
        switch (variant) {
        case 0 -> {
          // no vector property at all
        }
        case 1 -> sets.add(() -> d.set("vector", (Object) null));
        case 2 -> {
          final float[] wrong = new float[DIM + 1 + rnd.nextInt(3)];
          sets.add(() -> d.set("vector", wrong));
        }
        case 3 -> {
          vector = new float[DIM];
          zero = true;
          final float[] v = vector;
          sets.add(() -> d.set("vector", v));
        }
        default -> {
          vector = new float[DIM];
          for (int k = 0; k < DIM; k++)
            vector[k] = (float) rnd.nextGaussian();
          final float[] v = vector;
          sets.add(() -> d.set("vector", v));
        }
        }
        Collections.shuffle(sets, rnd);
        sets.forEach(Runnable::run);
        d.save();
        if (vector != null)
          present.add(new Expected(d.getIdentity(), vector, zero));
        else
          absent.add(d.getIdentity());
      }
    });
    assertThat(present.size()).isGreaterThan(40);
    assertThat(absent.size()).isGreaterThan(10);

    database.begin();
    try {
      final StoredVectorReader reader = new StoredVectorReader((DatabaseInternal) database, "vector", DIM, VectorEncoding.FLOAT32, false);
      final StoredVectorReader strict = new StoredVectorReader((DatabaseInternal) database, "vector", DIM, VectorEncoding.FLOAT32, true);
      final float[] scratch = new float[DIM];
      for (final Expected e : present) {
        assertThat(reader.readInPlaceOnly(e.rid)).as("%s read in place", e.rid).containsExactly(e.vector);
        assertThat(reader.read(e.rid, scratch)).as("%s read into a scratch array", e.rid).containsExactly(e.vector);
        if (e.zeroVector) {
          assertThat(strict.readInPlaceOnly(e.rid)).as("%s is all zeros: no vector when zero is unscorable", e.rid).isNull();
          assertThat(strict.read(e.rid, scratch)).isNull();
        } else
          assertThat(strict.read(e.rid, null)).as("%s", e.rid).containsExactly(e.vector);
      }
      for (final RID rid : absent) {
        assertThat(reader.read(rid, scratch)).as("%s holds no vector of %d dimensions", rid, DIM).isNull();
        assertThat(reader.readInPlaceOnly(rid)).isNull();
      }
    } finally {
      database.rollback();
    }
  }
}
