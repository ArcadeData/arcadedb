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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12724: a UNIQUE index created on a super type after its sub type got a UNIQUE index of its own on the same
 * property leaves two indexes over the sub type's bucket. The commit-time uniqueness check of an entry written to that bucket resolved
 * the index to look in from the type name and the property list, which for the sub type is always the sub type's own index, so the
 * entries of the super type's index were checked against an index that does not cover the super type's bucket: a key the super type
 * holds was accepted for a sub type record and the UNIQUE index held it twice. Each bucket index is checked against the type index
 * it belongs to, whichever order the two indexes were created in.
 */
class OverlappingUniqueIndexScopeTest extends TestHelper {

  @Test
  void aSubTypeRecordCannotTakeAKeyTheSuperTypeUniqueIndexHolds() {
    createHierarchy();
    database.command("sql", "CREATE INDEX ON Child12724 (p) UNIQUE");
    database.command("sql", "CREATE INDEX ON Parent12724 (p) UNIQUE");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Child12724").set("id", 3).set("p", 0L).save()))
        .isInstanceOf(DuplicatedKeyException.class).hasMessageContaining("Parent12724[p]");

    assertThat(database.countType("Parent12724", true)).isEqualTo(2L);
  }

  @Test
  void aSuperTypeRecordCannotTakeAKeyTheSubTypeHolds() {
    createHierarchy();
    database.command("sql", "CREATE INDEX ON Child12724 (p) UNIQUE");
    database.command("sql", "CREATE INDEX ON Parent12724 (p) UNIQUE");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Parent12724").set("id", 3).set("p", 1L).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType("Parent12724", true)).isEqualTo(2L);
  }

  @Test
  void theSubTypeKeepsItsOwnUniquenessToo() {
    createHierarchy();
    database.command("sql", "CREATE INDEX ON Child12724 (p) UNIQUE");
    database.command("sql", "CREATE INDEX ON Parent12724 (p) UNIQUE");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Child12724").set("id", 3).set("p", 1L).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType("Child12724", false)).isEqualTo(1L);
  }

  @Test
  void aHashIndexOnTheSuperTypeIsEnforcedForTheSubTypeToo() {
    createHierarchy();
    database.command("sql", "CREATE INDEX ON Child12724 (p) UNIQUE_HASH");
    database.command("sql", "CREATE INDEX ON Parent12724 (p) UNIQUE_HASH");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Child12724").set("id", 3).set("p", 0L).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType("Parent12724", true)).isEqualTo(2L);
  }

  @Test
  void anIndexOnTheSuperTypeAloneStillEnforcesItsSubTypes() {
    createHierarchy();
    database.command("sql", "CREATE INDEX ON Parent12724 (p) UNIQUE");

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Child12724").set("id", 3).set("p", 0L).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> database.newDocument("Child12724").set("id", 4).set("p", 7L).save());
    assertThat(database.countType("Parent12724", true)).isEqualTo(3L);
  }

  /** A parent holding p = 0 and a child holding p = 1: no duplicate yet, so either index can be created over them. */
  private void createHierarchy() {
    database.getSchema().createDocumentType("Parent12724").createProperty("p", Type.LONG);
    database.getSchema().createDocumentType("Child12724").addSuperType("Parent12724");
    database.transaction(() -> {
      database.newDocument("Parent12724").set("id", 1).set("p", 0L).save();
      database.newDocument("Child12724").set("id", 2).set("p", 1L).save();
    });
  }
}
