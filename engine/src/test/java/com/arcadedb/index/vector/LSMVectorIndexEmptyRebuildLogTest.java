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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.log.WarningCapture.LogLine;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A vector index whose pages hold no live vector is the most ordinary state there is: it has just been created, or
 * every record it covered has been deleted. The graph rebuild used to report both at SEVERE, and to blame a database
 * close it had never checked for. SEVERE is the level operators page on, so two of them per index creation is how a
 * class gets filtered out of alerting right before it has something real to say.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexEmptyRebuildLogTest extends TestHelper {

  private static final int DIMENSIONS = 8;

  @Test
  void creatingAnEmptyVectorIndexReportsNothingAtWarningOrAbove() {
    final List<LogLine> lines = WarningCapture.capture(Level.WARNING, () -> database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + "}");
    }));

    // The claim the old message made, checked against the state it claimed it from.
    assertThat(database.isOpen()).isTrue();
    assertRebuildIsNotAlarming(lines);
  }

  /**
   * The same for an index that had vectors and lost them all: the pages are full of tombstones, so the live set the
   * rebuild computes is empty exactly as it is for a fresh index.
   */
  @Test
  void emptyingAVectorIndexReportsNothingAtWarningOrAbove() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS + "}");
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Doc SET name = ?, embedding = ?", "v" + i, vector(i));
    });

    final LSMVectorIndex index = vectorIndex();
    index.buildVectorGraphNow();

    database.transaction(() -> database.command("sql", "DELETE FROM Doc"));

    final List<LogLine> lines = WarningCapture.capture(Level.WARNING, index::buildVectorGraphNow);

    assertThat(database.isOpen()).isTrue();
    assertRebuildIsNotAlarming(lines);
  }

  /**
   * Nothing at all, not merely nothing at SEVERE. Asserting only the absence of SEVERE would leave the other half of
   * the fix unguarded: a routine empty index reported at WARNING is still a line an operator has to triage, and the
   * WARNING arm of the rebuild is reserved for the one case that is a real disagreement between pages and memory.
   */
  private static void assertRebuildIsNotAlarming(final List<LogLine> lines) {
    assertThat(lines)
        .as("An empty vector index is a routine state and must not be reported at WARNING or above")
        .isEmpty();
  }

  private LSMVectorIndex vectorIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static float[] vector(final int seed) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (seed + 1) * (i + 1) / 10f;
    return v;
  }
}
