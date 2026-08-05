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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #5724: the key-type refusal added by #5677 lived in {@code HashIndexFactoryHandler.create()},
 * which is not the only way to build a hash index - the creation constructor of {@link HashIndex} is public, so a
 * caller reaching it directly wrote a file whose declared key type the bucket cannot encode. The problem then only
 * surfaced on the next open, as {@code loadMetadata} reporting corruption of an index that was never valid.
 * <p>
 * The guard now sits in the bucket's creation constructor, so the direct path is refused too. The load constructor
 * does NOT run it: an index already on disk must keep opening, which is what the reopen test pins.
 */
class Issue5724HashIndexKeyTypeGuardTest extends TestHelper {
  private static final String TYPE_NAME = "Doc";

  /**
   * The bypass the issue is about: constructing the index directly, without going through
   * {@code IndexFactory.createIndex()}. Before the guard moved into the constructor this returned a usable
   * {@link HashIndex} whose metadata page declared a key type no key could ever be encoded with.
   */
  @Test
  void directConstructionRefusesAnUnsupportedKeyType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE_NAME).createProperty("tags", Type.LIST);

      assertThatThrownBy(() -> newHashIndexDirectly("bypassed_list_key", new Type[] { Type.LIST }))//
          .isInstanceOf(IndexException.class)//
          .hasMessageContaining("LIST")//
          .hasMessageContaining("HASH")//
          .hasMessageNotContainingAny("corrupt", "rebuild");

      // the refusal must not register anything: the type is left without indexes
      assertThat(database.getSchema().getType(TYPE_NAME).getAllIndexes(false)).isEmpty();
      assertThat(database.getSchema().existsIndex("bypassed_list_key")).isFalse();
    });
  }

  /**
   * A composite key with a hole in it used to reach the constructor and die on {@code keyTypes[i].getBinaryType()}
   * with a bare {@link NullPointerException}. The guard names the problem instead.
   */
  @Test
  void directConstructionRefusesAKeyColumnWithoutAType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE_NAME);

      assertThatThrownBy(() -> newHashIndexDirectly("bypassed_null_key", new Type[] { Type.STRING, null }))//
          .isInstanceOf(IndexException.class)//
          .hasMessageContaining("bypassed_null_key")//
          .hasMessageContaining("no type");
    });
  }

  /**
   * The constructor assigns {@code this.keyTypes} from the guard's return value, so the guard has to hand back the
   * very array it was given: a copy, or a null on the accepting path, would silently change what the index reports.
   */
  @Test
  void theGuardReturnsTheAcceptedKeyTypesUnchanged() {
    final Type[] keyTypes = new Type[] { Type.STRING, Type.LINK };
    assertThat(HashIndexBucket.checkSupportedKeyTypes("accepted", keyTypes)).isSameAs(keyTypes);
  }

  /**
   * The upgrade case. The guard belongs to the creation constructor only: reopening a database whose hash indexes
   * were written by an earlier build must still work, so the load constructor must not consult it.
   */
  @Test
  void anExistingIndexStillOpensAfterAReopen() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Library");
      database.getSchema().createVertexType("Book").createProperty("library", Type.LINK);
      database.getSchema().getType("Book").createProperty("title", Type.STRING);
      database.getSchema().buildTypeIndex("Book", new String[] { "library", "title" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });

    final RID[] library = new RID[1];
    database.transaction(() -> {
      library[0] = database.newVertex("Library").set("name", "central").save().getIdentity();
      for (int i = 0; i < 20; i++)
        database.newVertex("Book").set("library", library[0]).set("title", "title-" + i).save();
    });

    reopenDatabase();

    database.transaction(() -> {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName("Book[library,title]");
      assertThat(index.getKeyTypes()).containsExactly(Type.LINK, Type.STRING);
      assertThat(index.countEntries()).isEqualTo(20L);
      assertThat(index.checkIntegrity()).isEmpty();

      try (final IndexCursor cursor = index.get(new Object[] { library[0], "title-7" })) {
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next().asVertex().getString("title")).isEqualTo("title-7");
      }
    });
  }

  /**
   * Builds a hash index the way the issue describes: straight through the public creation constructor, skipping
   * {@code HashIndexFactoryHandler} entirely.
   */
  private HashIndex newHashIndexDirectly(final String indexName, final Type[] keyTypes) {
    final DatabaseInternal db = (DatabaseInternal) database;
    return new HashIndex(db, indexName, true, db.getDatabasePath() + File.separator + indexName,
        ComponentFile.MODE.READ_WRITE, keyTypes, HashIndexBucket.DEF_PAGE_SIZE, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP);
  }
}
