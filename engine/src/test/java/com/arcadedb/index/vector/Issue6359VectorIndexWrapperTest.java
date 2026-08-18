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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6359: a vector bucket sub-index has to answer which {@link TypeIndex} it belongs to.
 * <p>
 * {@link LSMVectorIndex#getTypeIndex()} used to answer {@code null}, with the comment "Not applicable for this index
 * type", which was never true - a vector index IS registered under a wrapper, and every sibling family
 * ({@code LSMSparseVectorIndex}, {@code LSMTreeIndex}, {@code HashIndex}, ...) has always wired it.
 * <p>
 * Asserted here on the DROP path rather than through the {@code addSuperType} scenario that first exposed it, because
 * this half is the wider one: {@code LocalSchema.dropIndexInternal} skips {@code parentTypeIndex.removeIndexOnBucket}
 * when the answer is null, so EVERY path that drops a vector sub-index used to leave the wrapper holding a reference
 * to an index that no longer exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6359VectorIndexWrapperTest extends TestHelper {

  /** Two buckets, so dropping one sub-index leaves the wrapper alive to be asked about the other. */
  @Test
  @Timeout(60)
  void aVectorSubIndexNamesItsWrapperAndIsReleasedByItOnDrop() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("V", 2).createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql",
          "CREATE INDEX ON V (embedding) LSM_VECTOR METADATA {\"dimensions\": 4, \"similarity\": \"cosine\"}").close();
    });

    final TypeIndex wrapper = (TypeIndex) database.getSchema().getIndexByName("V[embedding]");
    final IndexInternal[] subIndexes = wrapper.getIndexesOnBuckets();
    assertThat(subIndexes).as("one sub-index per bucket").hasSize(2);

    for (final IndexInternal sub : subIndexes)
      assertThat(sub.getTypeIndex()).as("a vector sub-index names the wrapper it was registered under").isSameAs(wrapper);

    final IndexInternal dropped = subIndexes[0];
    database.getSchema().dropIndex(dropped.getName());

    assertThat(wrapper.getIndexesOnBuckets()).as("and the wrapper lets go of it when it is dropped")
        .doesNotContain(dropped).hasSize(1);
  }
}
