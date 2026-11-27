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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12753: an LSM_TREE index on an EMBEDDED property is now refused at creation. A database created before the fix can
 * already hold one, and it must keep opening: the load path does not go through the factory handler that refuses it, so the
 * index can still be found and dropped.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12753LegacyEmbeddedKeyIndexTest extends TestHelper {

  @Test
  void databaseHoldingAnEmbeddedKeyIndexStillOpensAndTheIndexCanBeDropped() {
    database.getSchema().createDocumentType("Emb");
    database.getSchema().createDocumentType("D").createProperty("p", Type.EMBEDDED, "Emb");

    // Builds the index the way a release without the refusal did, by bypassing the handler's key type check
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.indexFactory.register(Schema.INDEX_TYPE.LSM_TREE.name(),
        builder -> new LSMTreeIndex(builder.getDatabase(), builder.getIndexName(), builder.isUnique(), builder.getFilePath(),
            ComponentFile.MODE.READ_WRITE, builder.getKeyTypes(), builder.getLsmTreePageSize(), builder.getNullStrategy()));
    try {
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "D", "p");
    } finally {
      schema.indexFactory.register(Schema.INDEX_TYPE.LSM_TREE.name(), new LSMTreeIndex.LSMTreeIndexFactoryHandler());
    }

    database.transaction(() -> database.command("sql", "INSERT INTO D SET p = {'@type':'Emb','x':1}"));

    reopenDatabase();

    assertThat(database.getSchema().getType("D").getAllIndexes(true)).hasSize(1);
    final TypeIndex index = database.getSchema().getType("D").getAllIndexes(true).iterator().next();
    final String indexName = index.getName();
    assertThat(index.getKeyTypes()).containsExactly(Type.EMBEDDED);
    assertThat(database.countType("D", false)).isEqualTo(1);

    database.getSchema().dropIndex(indexName);
    assertThat(database.getSchema().getType("D").getAllIndexes(true)).isEmpty();

    database.transaction(() -> database.command("sql", "INSERT INTO D SET p = {'@type':'Emb','x':2}"));
    assertThat(database.countType("D", false)).isEqualTo(2);
  }
}
