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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12753: an LSM_TREE index on an EMBEDDED property was accepted at CREATE INDEX and then failed the second insert with a
 * ClassCastException in {@code BinaryComparator.compareTo} (an embedded document is not a comparable key). HASH and FULL_TEXT
 * already refuse the key type at creation; LSM_TREE must too.
 */
class Issue12753EmbeddedKeyIndexTest extends TestHelper {

  private DocumentType embeddedOwner() {
    database.getSchema().createDocumentType("Emb");
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("p", Type.EMBEDDED, "Emb");
    return type;
  }

  @Test
  void sqlCreateIndexOnEmbeddedPropertyIsRefused() {
    embeddedOwner();

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON D (p) NOTUNIQUE")).isInstanceOf(IndexException.class)
        .hasMessageContaining("EMBEDDED").hasMessageContaining("LSM_TREE");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON D (p) UNIQUE")).hasMessageContaining("EMBEDDED");

    assertThat(database.getSchema().getType("D").getAllIndexes(true)).isEmpty();
  }

  @Test
  void apiCreateIndexOnEmbeddedPropertyIsRefusedAndLeavesNoFile() {
    embeddedOwner();

    assertThatThrownBy(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "D", "p")).isInstanceOf(
        IndexException.class);

    assertThat(database.getSchema().getType("D").getAllIndexes(true)).isEmpty();
    assertThat(database.getSchema().getIndexes()).isEmpty();
  }

  @Test
  void refusedIndexLeavesTheTypeWritable() {
    embeddedOwner();
    assertThatThrownBy(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "D", "p")).isInstanceOf(
        IndexException.class);

    for (int i = 0; i < 3; i++) {
      final int x = i;
      database.transaction(() -> database.command("sql", "INSERT INTO D SET p = {'@type':'Emb','x':" + x + "}"));
    }
    assertThat(database.countType("D", false)).isEqualTo(3);
  }

  @Test
  void byItemIndexOnAListOfEmbeddedDocumentsIsStillAccepted() {
    // A BY ITEM index over a LIST of embedded documents is keyed by STRING, so the refusal of the EMBEDDED key type does not reach it
    database.getSchema().createDocumentType("Emb");
    database.getSchema().createDocumentType("L").createProperty("items", Type.LIST, "Emb");

    database.command("sql", "CREATE INDEX ON L (items BY ITEM) NOTUNIQUE");

    final TypeIndex index = database.getSchema().getType("L").getAllIndexes(true).iterator().next();
    assertThat(index.getKeyTypes()).containsExactly(Type.STRING);
  }

  @Test
  void indexOnAFieldOfTheEmbeddedDocumentIsStillAccepted() {
    embeddedOwner();
    database.getSchema().getType("Emb").createProperty("x", Type.INTEGER);

    database.command("sql", "CREATE INDEX ON D (`p.x`) NOTUNIQUE");

    for (int i = 0; i < 3; i++) {
      final int x = i;
      database.transaction(() -> database.command("sql", "INSERT INTO D SET p = {'@type':'Emb','x':" + x + "}"));
    }
    assertThat(database.countType("D", false)).isEqualTo(3);
  }
}
