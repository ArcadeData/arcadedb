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
package com.arcadedb;

import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4501 - "Bucket with id 'NNN' was not found".
 * <p>
 * A dangling index entry can reference an RID whose bucket no longer exists (for example the record/bucket was removed but the index entry
 * survived, or the type still references a bucket that is missing from the schema files registry). Resolving that RID during a query used to
 * throw a {@code SchemaException} ("Bucket with id 'NNN' was not found"), which bubbled up wrapped as the user-visible "Error on transaction
 * commit". A missing bucket means the record simply does not exist, so {@link com.arcadedb.database.Database#lookupByRID} now reports it as
 * {@link RecordNotFoundException} (skipped by index scans) and {@code existsRecord} returns {@code false}, instead of aborting the query.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4501Test extends TestHelper {

  // An RID located in a bucket id that is guaranteed not to exist in the schema files registry.
  private static final RID DANGLING_RID = new RID(9999, 0);

  public Issue4501Test() {
    autoStartTx = true;
  }

  // Some tests deliberately inject a corrupt (dangling) index entry, so the integrity check at teardown would fail by design.
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void lookupByRidToMissingBucketReportsRecordNotFound() {
    // Before the fix this threw SchemaException("Bucket with id '9999' was not found").
    assertThatThrownBy(() -> database.lookupByRID(DANGLING_RID, true)).isInstanceOf(RecordNotFoundException.class);
    assertThatThrownBy(() -> database.lookupByRID(DANGLING_RID, false)).isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  void existsRecordOnMissingBucketReturnsFalse() {
    // Before the fix this threw SchemaException("Bucket with id '9999' was not found").
    assertThat(database.existsRecord(DANGLING_RID)).isFalse();
  }

  @Test
  void selectSkipsDanglingIndexEntryToMissingBucket() {
    final VertexType type = database.getSchema().createVertexType("Company");
    type.createProperty("entityId", Type.STRING);
    final TypeIndex index = (TypeIndex) database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Company", "entityId");

    database.command("sql", "INSERT INTO Company SET entityId = '11111111'");
    database.command("sql", "INSERT INTO Company SET entityId = '22222222'");
    database.commit();

    // Inject a dangling index entry into a bucket sub-index: key "88888888" -> RID located in a bucket that does not exist.
    final IndexInternal bucketIndex = index.getSubIndexes().getFirst();
    database.begin();
    bucketIndex.put(new Object[] { "88888888" }, new RID[] { DANGLING_RID });
    database.commit();

    // The query must not abort with "Bucket with id '9999' was not found"; the dangling entry is skipped.
    assertThatCode(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Company WHERE entityId = '88888888'");
      assertThat(rs.hasNext()).isFalse();
    }).doesNotThrowAnyException();

    // Valid lookups on other keys keep working unchanged.
    final ResultSet ok = database.query("sql", "SELECT FROM Company WHERE entityId = '11111111'");
    assertThat(ok.hasNext()).isTrue();
    assertThat(ok.next().<String>getProperty("entityId")).isEqualTo("11111111");
    assertThat(ok.hasNext()).isFalse();
  }

  /**
   * Write-side repair (issue #4501 follow-up): a dangling entry in a UNIQUE index used to block all writes on that key. INSERTing a real record
   * for the dangling key failed with {@code DuplicatedKeyException} (the index still reported the key as present) while the unreachable record
   * could neither be read nor updated. The unique-constraint check now probes the conflicting RID and, when it cannot be loaded, removes the
   * dangling entry and lets the write proceed instead of aborting the transaction.
   */
  @Test
  void insertRepairsDanglingUniqueEntryInsteadOfFailing() {
    final VertexType type = database.getSchema().createVertexType("Company");
    type.createProperty("entityId", Type.STRING);
    final TypeIndex index = (TypeIndex) database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Company", "entityId");

    database.command("sql", "INSERT INTO Company SET entityId = '11111111'");
    database.commit();

    injectDanglingEntry(index, "88888888");

    // Before the fix this aborted with DuplicatedKeyException; now the dangling entry is repaired and the new record is stored.
    assertThatCode(() -> {
      database.begin();
      database.command("sql", "INSERT INTO Company SET entityId = '88888888'");
      database.commit();
    }).doesNotThrowAnyException();

    assertIndexPointsToRealRecordOnly(index, "88888888");
  }

  /**
   * Same scenario reproduced through the exact statement reported in issue #4501: an UPSERT on the dangling key. The fetch skips the unreachable
   * record (so no match is found), a fresh record is created, and the unique-constraint check repairs the dangling entry at commit time instead
   * of failing with NoSuchElementException / DuplicatedKeyException.
   */
  @Test
  void upsertRepairsDanglingUniqueEntryInsteadOfFailing() {
    final VertexType type = database.getSchema().createVertexType("Company");
    type.createProperty("entityId", Type.STRING);
    final TypeIndex index = (TypeIndex) database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Company", "entityId");

    database.command("sql", "INSERT INTO Company SET entityId = '11111111'");
    database.commit();

    injectDanglingEntry(index, "88888888");

    assertThatCode(() -> {
      database.begin();
      database.command("sql", "UPDATE Company SET entityId = '88888888', name = 'ACME' UPSERT WHERE entityId = '88888888'");
      database.commit();
    }).doesNotThrowAnyException();

    assertIndexPointsToRealRecordOnly(index, "88888888");

    final ResultSet rs = database.query("sql", "SELECT FROM Company WHERE entityId = '88888888'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("ACME");
    assertThat(rs.hasNext()).isFalse();
  }

  // Injects a dangling entry into a bucket sub-index: key -> RID located in a bucket that does not exist.
  private void injectDanglingEntry(final TypeIndex index, final String key) {
    final IndexInternal bucketIndex = index.getSubIndexes().getFirst();
    database.begin();
    bucketIndex.put(new Object[] { key }, new RID[] { DANGLING_RID });
    database.commit();
  }

  // Verifies the key resolves to a single, valid record (not the dangling RID), i.e. the index was repaired.
  private void assertIndexPointsToRealRecordOnly(final TypeIndex index, final String key) {
    final ResultSet rs = database.query("sql", "SELECT FROM Company WHERE entityId = '" + key + "'");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat(r.<String>getProperty("entityId")).isEqualTo(key);
    assertThat(rs.hasNext()).isFalse();

    final IndexCursor cursor = index.get(new Object[] { key });
    assertThat(cursor.hasNext()).isTrue();
    assertThat(cursor.next().getIdentity()).isNotEqualTo(DANGLING_RID);
    assertThat(cursor.hasNext()).isFalse();
  }
}
