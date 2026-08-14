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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code ManualIndexBuilder} is the only route by which a CALLER-SUPPLIED index name reaches
 * {@code LocalSchema.indexMap} without passing through the schema's own null-guarded accessors, and it had no name
 * validation at all: a null name was accepted all the way into the map, leaving an index nothing could ever look up
 * again. Issue #6105 made that map a {@code ConcurrentHashMap} (a compaction re-keys it from the async executor, so
 * the publication has to be safe on its own rather than resting on a lock the two sides happen to share), which
 * turns the same call into an NPE from inside the map. Neither is an acceptable answer: say what is wrong instead.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class ManualIndexNameGuardTest extends TestHelper {

  @Test
  void aManualIndexCannotBeCreatedWithoutAName() {
    final int indexesBefore = database.getSchema().getIndexes().length;

    assertThatThrownBy(() -> database.getSchema().buildManualIndex(null, new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .create())
        .as("a null name must be refused with an explanation, not with an NPE from inside the schema's index map")
        .isInstanceOf(DatabaseMetadataException.class)
        .hasMessageContaining("without a name");

    assertThat(database.getSchema().getIndexes())
        .as("the refused creation must leave nothing behind in the schema").hasSize(indexesBefore);
  }

  /** The guard must refuse only a missing name: an ordinary manual index still builds and is registered. */
  @Test
  void aNamedManualIndexIsStillCreated() {
    final Index index = database.getSchema().buildManualIndex("namedManualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .create();

    assertThat(index.getName()).isEqualTo("namedManualIdx");
    assertThat(database.getSchema().existsIndex("namedManualIdx")).isTrue();
    assertThat(database.getSchema().getIndexByName("namedManualIdx")).isSameAs(index);
  }
}
