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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryTypes;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Diagnostics regression for the customer issue behind PR #4637 (Locstat): a corrupted hash index metadata
 * page surfaced only as the cryptic {@code IndexException: Unsupported key type for hash index: -108} deep in
 * a search, with no indication of which index was affected or how to recover, and the corruption was never
 * detected at open time.
 *
 * <p>These tests inject the exact corruption (an invalid key-type byte / key-count byte on the metadata page)
 * and assert the hardened behavior:
 * <ul>
 *   <li>the database still <b>opens</b> (so the corrupt index can be dropped and rebuilt without recreating the
 *       whole database), and</li>
 *   <li>using the corrupt index fails with an <b>actionable</b> error that names the index, dumps the loaded key
 *       types, and tells the operator to rebuild it - instead of a bare "-108".</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HashIndexMetadataCorruptionTest extends TestHelper {
  private static final String TYPE_NAME = "Account";
  private static final int    ACCOUNTS  = 16;

  private String indexName;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType account = database.getSchema().createVertexType(TYPE_NAME);
      account.createProperty("bank", Type.STRING);
      account.createProperty("number", Type.STRING);
      final Index index = database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "bank", "number" })
          .withType(Schema.INDEX_TYPE.HASH)
          .withUnique(true)
          .create();
      indexName = index.getName();

      for (int n = 0; n < ACCOUNTS; n++)
        database.newVertex(TYPE_NAME).set("bank", "bank_0").set("number", String.format("%06d", n)).save();
    });
  }

  @Test
  void corruptKeyTypeSurfacesActionableErrorOnLookup() {
    // Overwrite the first key-type byte on the metadata page with -108 (0x94) - exactly the corruption the
    // customer hit. The index keeps a valid key count (2), so the bad type is loaded and only blows up when a
    // stored entry is parsed during a search.
    corruptMetadataByte(HashIndexBucket.META_KEY_TYPES_START, (byte) -108);

    // Reopen so loadMetadata() re-reads the corrupt byte from disk into the cached key types.
    reopenDatabase();

    // The database must still be open (corruption is logged, not fatal at load) so the index can be repaired.
    assertThat(database.isOpen()).isTrue();

    database.transaction(() -> assertThatThrownBy(() -> {
      final IndexCursor cursor = database.getSchema().getIndexByName(indexName).get(new Object[] { "bank_0", "000000" });
      cursor.hasNext();
    }).isInstanceOf(IndexException.class)
        .hasMessageContaining(TYPE_NAME)        // names the affected index (the underlying hash bucket of the Account type)
        .hasMessageContaining("-108")           // still reports the offending byte
        .hasMessageContaining("0x94")           // and its hex form
        .hasMessageContaining("rebuild"));      // tells the operator how to recover
  }

  @Test
  void corruptKeyCountKeepsDatabaseOpenableAndRepairable() {
    // A garbage key-count byte (148) used to either throw IndexOutOfBounds deep in loadMetadata or silently load
    // junk key types. It must now be clamped: the database stays openable so the operator can rebuild the index.
    corruptMetadataByte(HashIndexBucket.META_NUMBER_OF_KEYS, (byte) 0x94);

    reopenDatabase();
    assertThat(database.isOpen()).isTrue();

    // Recovery WITHOUT recreating the database: drop the corrupt index and rebuild it from the intact vertices.
    database.getSchema().dropIndex(indexName);
    database.transaction(() -> database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "bank", "number" })
        .withType(Schema.INDEX_TYPE.HASH)
        .withUnique(true)
        .create());

    // The rebuilt index resolves every account again.
    database.transaction(() -> {
      for (int n = 0; n < ACCOUNTS; n++) {
        final IndexCursor cursor = database.getSchema().getIndexByName(indexName)
            .get(new Object[] { "bank_0", String.format("%06d", n) });
        assertThat(cursor.hasNext()).withFailMessage("Account %06d not found after rebuild", n).isTrue();
      }
    });
  }

  @Test
  void checkDatabaseDetectsCorruptHashIndexMetadata() {
    corruptMetadataByte(HashIndexBucket.META_KEY_TYPES_START, (byte) -108);
    reopenDatabase();

    final ResultSet rs = database.command("sql", "check database");
    final Result row = rs.next();

    final Collection<String> corruptedIndexes = row.getProperty("corruptedIndexes");
    assertThat(corruptedIndexes).withFailMessage("CHECK DATABASE did not flag the corrupt index, got %s", corruptedIndexes)
        .anyMatch(name -> name.contains(TYPE_NAME));

    final Collection<String> warnings = row.getProperty("warnings");
    assertThat(warnings).anyMatch(w -> w.contains("invalid key type") && w.contains("-108"));
  }

  @Test
  void checkDatabaseFixRebuildsCorruptHashIndex() {
    corruptMetadataByte(HashIndexBucket.META_KEY_TYPES_START, (byte) -108);
    reopenDatabase();

    database.command("sql", "check database fix");

    // A second pass must report a clean index: FIX rebuilt it from the intact vertices.
    final Result recheck = database.command("sql", "check database").next();
    final Collection<String> corruptedIndexes = recheck.getProperty("corruptedIndexes");
    assertThat(corruptedIndexes).isEmpty();

    // ...and lookups resolve every account again.
    database.transaction(() -> {
      for (int n = 0; n < ACCOUNTS; n++) {
        final IndexCursor cursor = database.getSchema().getIndexByName(indexName)
            .get(new Object[] { "bank_0", String.format("%06d", n) });
        assertThat(cursor.hasNext()).withFailMessage("Account %06d not found after CHECK FIX", n).isTrue();
      }
    });
  }

  @Test
  void isSupportedKeyTypeRejectsCorruptValues() {
    assertThat(HashIndexBucket.isSupportedKeyType((byte) -108)).isFalse();
    assertThat(HashIndexBucket.isSupportedKeyType((byte) 99)).isFalse();
    assertThat(HashIndexBucket.isSupportedKeyType(BinaryTypes.TYPE_STRING)).isTrue();
    assertThat(HashIndexBucket.isSupportedKeyType(BinaryTypes.TYPE_LONG)).isTrue();
  }

  /**
   * Overwrites a single content-relative byte of the hash index metadata page (page 0) and commits, so the change
   * is flushed to disk on the next reopen.
   */
  private void corruptMetadataByte(final int contentOffset, final byte value) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = ((IndexInternal) db.getSchema().getIndexByName(indexName)).getFileIds().getFirst();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        page.writeByte(contentOffset, value);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
