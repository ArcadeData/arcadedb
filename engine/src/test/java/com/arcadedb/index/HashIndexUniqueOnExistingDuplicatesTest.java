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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test: creating a UNIQUE_HASH index on a type that already contains
 * duplicate key values must fail instead of silently succeeding with duplicates in place.
 */
class HashIndexUniqueOnExistingDuplicatesTest extends TestHelper {

  @Test
  void createUniqueHashIndexOnExistingDuplicatesMustFail() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("OrganizationProvider");
      type.createProperty("organizationId", String.class);
    });

    database.transaction(() -> {
      for (int i = 0; i < 20; ++i) {
        final MutableDocument doc = database.newDocument("OrganizationProvider");
        doc.set("organizationId", "gtm-orders");
        doc.set("id", i);
        doc.save();
      }
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("OrganizationProvider", new String[] { "organizationId" })
        .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(database.getSchema().existsIndex("OrganizationProvider[organizationId]")).isFalse();
  }

  @Test
  void createUniqueHashIndexViaSqlOnExistingDuplicatesMustFail() {
    database.command("sql", "CREATE DOCUMENT TYPE OrganizationProvider");
    database.command("sql", "CREATE PROPERTY OrganizationProvider.organizationId STRING");

    database.transaction(() -> {
      for (int i = 0; i < 20; ++i) {
        final MutableDocument doc = database.newDocument("OrganizationProvider");
        doc.set("organizationId", "gtm-orders");
        doc.set("id", i);
        doc.save();
      }
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON OrganizationProvider (organizationId) UNIQUE_HASH"))
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(database.getSchema().existsIndex("OrganizationProvider[organizationId]")).isFalse();
  }

  @Test
  void createUniqueLsmTreeIndexOnExistingDuplicatesMustFail() {
    // Baseline: same behavior for LSM_TREE, kept for comparison with HASH.
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("LsmDupType");
      type.createProperty("code", String.class);
    });

    database.transaction(() -> {
      for (int i = 0; i < 20; ++i) {
        final MutableDocument doc = database.newDocument("LsmDupType");
        doc.set("code", "same");
        doc.set("id", i);
        doc.save();
      }
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("LsmDupType", new String[] { "code" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create())
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(database.getSchema().existsIndex("LsmDupType[code]")).isFalse();
  }
}
