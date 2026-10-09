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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9331: the marker of an index being populated is counted, so two builds on the same properties keep the index out of
 * the queries until the last one is over, and a build on a subtype hierarchy marks the type the index belongs to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9331IndexUnderConstructionTest extends TestHelper {
  @Test
  void theMarkerOutlivesTheFirstOfTwoBuildsOnTheSameProperties() {
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().createDocumentType("Marked");
    final List<String> properties = List.of("id");

    assertThat(type.isIndexUnderConstruction(properties)).isFalse();

    type.beginIndexConstruction(properties);
    type.beginIndexConstruction(properties);
    assertThat(type.isIndexUnderConstruction(properties)).isTrue();

    type.endIndexConstruction(properties);
    assertThat(type.isIndexUnderConstruction(properties)).as("the second build is still populating").isTrue();

    type.endIndexConstruction(properties);
    assertThat(type.isIndexUnderConstruction(properties)).isFalse();
    assertThat(type.isIndexUnderConstruction(List.of("other"))).isFalse();
  }

  /** Pins the wording the stale-index detection reads: a change in the file manager's message must fail here, not in production. */
  @Test
  void theFileManagerMessageIsWhatTheStaleIndexDetectionMatches() {
    assertThatThrownBy(() -> ((DatabaseInternal) database).getFileManager().getFile(Integer.MAX_VALUE))
        .isInstanceOfSatisfying(IllegalArgumentException.class, e -> assertThat(TypeIndex.isFileNotFound(e)).isTrue());
    assertThat(TypeIndex.isFileNotFound(new IllegalArgumentException("bad key"))).isFalse();
    assertThat(TypeIndex.isFileNotFound(new IllegalArgumentException())).isFalse();
  }

  @Test
  void theMarkerBelongsToTheTypeBeingIndexed() {
    final LocalDocumentType first = (LocalDocumentType) database.getSchema().createDocumentType("First");
    final LocalDocumentType second = (LocalDocumentType) database.getSchema().createDocumentType("Second");
    final List<String> properties = List.of("id");

    first.beginIndexConstruction(properties);

    assertThat(first.isIndexUnderConstruction(properties)).isTrue();
    assertThat(second.isIndexUnderConstruction(properties)).as("another type with the same property names").isFalse();

    first.endIndexConstruction(properties);
  }

  @Test
  void anIndexOnAParentTypeIsReadyOnlyOnceItsBuildIsOver() {
    final DocumentType parent = database.getSchema().createDocumentType("Parent");
    parent.createProperty("id", Type.LONG);
    database.getSchema().createDocumentType("Child").addSuperType(parent);
    final LocalDocumentType local = (LocalDocumentType) parent;

    database.transaction(() -> {
      for (long i = 0; i < 20; i++)
        database.newDocument("Child").set("id", i).save();
    });

    final TypeIndex[] seenDuringBuild = new TypeIndex[1];
    final boolean[] readyDuringBuild = new boolean[] { true };
    parent.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, new String[] { "id" }, 262_144, (document, total) -> {
      seenDuringBuild[0] = parent.getPolymorphicIndexByProperties("id");
      readyDuringBuild[0] = seenDuringBuild[0] != null && seenDuringBuild[0].isReadyForQueries();
    });

    assertThat(seenDuringBuild[0]).as("the build callback ran and the index was already registered").isNotNull();
    assertThat(readyDuringBuild[0]).as("not offered to queries while it is being populated").isFalse();
    assertThat(local.isIndexUnderConstruction(List.of("id"))).isFalse();
    assertThat(parent.getPolymorphicIndexByProperties("id").isReadyForQueries()).isTrue();
  }
}
