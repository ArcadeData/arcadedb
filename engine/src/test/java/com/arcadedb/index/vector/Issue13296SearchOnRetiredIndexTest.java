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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13296: a Raft follower applies a replicated {@code COMPACT INDEX} by deleting the superseded component files and only then
 * reloading the schema, so a search that still resolves the previous generation of a sub-index reads a file that is gone. The
 * sub-index, cold after the previous schema load of the same statement, parsed no entries from it and the search answered normally
 * without its rows: an exhaustive search lacked whole buckets for the 1 to 2 seconds the apply takes. It now fails with a retryable
 * exception instead.
 * <p>
 * The test does by hand what the apply does to the files of one sub-index of a reopened (cold) 4-bucket index.
 */
class Issue13296SearchOnRetiredIndexTest extends TestHelper {
  private static final int DIMENSIONS = 8;
  private static final int BUCKETS    = 4;
  private static final int RECORDS    = 400;
  private static final String EXHAUSTIVE = "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('Doc[v]', :q, :k)))";

  @Test
  void anExhaustiveSearchOverARetiredSubIndexFailsInsteadOfAnsweringShort() throws Exception {
    final Random random = new Random(13296);
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Doc").withTotalBuckets(BUCKETS).create().createProperty("v", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (v) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"quantization\": \"INT8\", \"storeVectorsInGraph\": true}");
    });
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("Doc").set("v", gaussian(random)).save();
    });
    final float[] query = gaussian(random);
    assertThat(exhaustiveRows(query)).as("the warm index answers every record").isEqualTo(RECORDS);

    // Reopened: no location index is materialised and no graph is resident, as for the index objects a schema load has just made
    reopenDatabase();
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[v]");
    final Index subIndex = typeIndex.getIndexesOnBuckets()[1];
    final int fileId = ((IndexInternal) subIndex).getFileId();

    final DatabaseInternal internal = (DatabaseInternal) database;
    internal.getPageManager().deleteFile(internal, fileId);
    internal.getFileManager().dropFile(fileId);
    internal.getSchema().getEmbedded().removeFile(fileId);

    assertThatThrownBy(() -> exhaustiveRows(query)).as("a search over a retired sub-index is refused, not answered short")
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("retired by a schema change");
  }

  /** The files of one sub-index are gone by design: the integrity check at the end would only report them. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  private int exhaustiveRows(final float[] query) {
    final Set<String> rids = new HashSet<>();
    try (final ResultSet resultSet = database.query("sql", EXHAUSTIVE, Map.of("q", query, "k", 100_000))) {
      while (resultSet.hasNext())
        rids.add(String.valueOf((Object) resultSet.next().getProperty("rid")));
    }
    return rids.size();
  }

  private static float[] gaussian(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }
}
