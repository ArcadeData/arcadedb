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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UniqueIndexConstraintTest {
  /**
   * Regression for #2590: Unique constraints not enforced during UPDATE operations.
   */
  @Nested
  class UniqueConstraintOnUpdate extends TestHelper {

    public UniqueConstraintOnUpdate() {
      autoStartTx = true;
    }

    @Test
    void databaseUpdateToNullThenToDuplicate() {
      // Create vertex type with unique field
      database.command("sql", "CREATE VERTEX TYPE NullRemoteTest");
      database.command("sql", "CREATE PROPERTY NullRemoteTest.licenseKey STRING");
      database.command("sql", "CREATE INDEX ON NullRemoteTest (licenseKey) UNIQUE NULL_STRATEGY SKIP");

      // Insert vertices
      database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-AAA', product = 'Product A'");
      database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-BBB', product = 'Product B'");
      database.command("sql", "INSERT INTO NullRemoteTest SET licenseKey = 'LIC-CCC', product = 'Product C'");

      // Update from LIC-BBB to null (should succeed)
      ResultSet updateToNull = database.command("sql", "UPDATE NullRemoteTest SET licenseKey = null WHERE licenseKey = 'LIC-BBB'");
      assertThat(updateToNull.hasNext()).isTrue();

      // Update from LIC-CCC to duplicate existing value LIC-AAA (should fail)
      assertThatThrownBy(() ->
        database.command("sql", "UPDATE NullRemoteTest SET licenseKey = 'LIC-AAA' WHERE licenseKey = 'LIC-CCC'")).isInstanceOf(DuplicatedKeyException.class)
          .as("UPDATE to duplicate value should enforce unique constraint");

      database.rollback();
    }

    @Test
    void uniqueConstraintOnUpdate() {
      // Create vertex type with unique field
      final VertexType vertexType = database.getSchema().createVertexType("TestVertex");
      vertexType.createProperty("uniqueField", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "TestVertex", "uniqueField");

      // Insert first vertex with value "A"
      final MutableVertex vertex1 = database.newVertex("TestVertex");
      vertex1.set("uniqueField", "A");
      vertex1.save();

      // Insert second vertex with value "B"
      final MutableVertex vertex2 = database.newVertex("TestVertex");
      vertex2.set("uniqueField", "B");
      vertex2.save();

      // Verify INSERT of duplicate value is correctly prevented
      assertThatThrownBy(() -> {
        final MutableVertex vertex3 = database.newVertex("TestVertex");
        vertex3.set("uniqueField", "A"); // Duplicate value
        vertex3.save();
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("INSERT should have thrown DuplicatedKeyException");

      // UPDATE second vertex to use value "A" - should fail with DuplicatedKeyException
      // Reload vertex2 to ensure it's in a valid state
      final MutableVertex vertex2Reloaded = database.lookupByRID(vertex2.getIdentity(), true)
          .asVertex()
          .modify();
      assertThatThrownBy(() -> {
        vertex2Reloaded.set("uniqueField", "A");
        vertex2Reloaded.save();
      }).isInstanceOf(DuplicatedKeyException.class)
          .as("UPDATE should have thrown DuplicatedKeyException");

      database.rollback();
    }

    @Test
    void validUpdateToNonDuplicateValue() {
      // Create vertex type with unique field
      final VertexType vertexType = database.getSchema().createVertexType("TestVertex3");
      vertexType.createProperty("uniqueField", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "TestVertex3", "uniqueField");

      // Insert vertex
      final MutableVertex vertex1 = database.newVertex("TestVertex3");
      vertex1.set("uniqueField", "A");
      vertex1.save();

      // Update to a new unique value - should succeed
      vertex1.set("uniqueField", "C");
      vertex1.save();

      // Verify the update succeeded
      assertThat(vertex1.get("uniqueField")).isEqualTo("C");
    }
  }

  /**
   * Regression for #4501: "Bucket with id 'NNN' was not found".
   * <p>
   * A dangling index entry can reference an RID whose bucket no longer exists (for example the record/bucket was removed but the index entry
   * survived, or the type still references a bucket that is missing from the schema files registry). Resolving that RID during a query used to
   * throw a {@code SchemaException} ("Bucket with id 'NNN' was not found"), which bubbled up wrapped as the user-visible "Error on transaction
   * commit". A missing bucket means the record simply does not exist, so {@link com.arcadedb.database.Database#lookupByRID} now reports it as
   * {@link RecordNotFoundException} (skipped by index scans) and {@code existsRecord} returns {@code false}, instead of aborting the query.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DanglingIndexEntry extends TestHelper {

    // An RID located in a bucket id that is guaranteed not to exist in the schema files registry.
    private static final RID DANGLING_RID = new RID(9999, 0);

    public DanglingIndexEntry() {
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

  /**
   * Regression for #9339: an UPSERT whose WHERE condition is not an equality on every property of a UNIQUE
   * index (non-unique, missing or partially matched composite index, range, IN, OR, duplicated predicate) must say so in
   * the error, instead of the generic "must involve an index" wording that misled users who did have an index on the
   * property. A composite UNIQUE index is usable only when the WHERE matches all its properties.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UpsertIndex extends TestHelper {

    @Test
    void upsertOnUniqueIndexWorksWithNamedAndPositionalParameters() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.code STRING");
      database.command("sql", "CREATE INDEX ON Doc (code) UNIQUE");

      database.transaction(() -> {
        database.command("sql", "UPDATE Doc SET name = :a, code = :b UPSERT WHERE code = :b", Map.of("a", "x", "b", "c1"));
        database.command("sql", "UPDATE Doc SET name = ?, code = ? UPSERT WHERE code = ?", "y", "c1", "c1");
      });
      assertThat(database.countType("Doc", false)).isEqualTo(1);
      try (final ResultSet rs = database.query("sql", "SELECT code, name FROM Doc")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("code")).isEqualTo("c1");
        assertThat(row.<String>getProperty("name")).isEqualTo("y");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void upsertWithoutUniqueIndexExplainsTheRequirement() {
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "CREATE PROPERTY Plain.code STRING");
      database.command("sql", "CREATE INDEX ON Plain (code) NOTUNIQUE");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Plain SET code = ? UPSERT WHERE code = ?", "c1", "c1")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
    }

    @Test
    void upsertWithoutAnyIndexExplainsTheRequirement() {
      database.command("sql", "CREATE DOCUMENT TYPE NoIdx");
      database.command("sql", "CREATE PROPERTY NoIdx.code STRING");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE NoIdx SET code = ? UPSERT WHERE code = ?", "c1", "c1")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
    }

    @Test
    void upsertOnCompositeUniqueIndexNeedsAllItsProperties() {
      database.command("sql", "CREATE DOCUMENT TYPE Comp");
      database.command("sql", "CREATE PROPERTY Comp.tenant STRING");
      database.command("sql", "CREATE PROPERTY Comp.code STRING");
      database.command("sql", "CREATE INDEX ON Comp (tenant, code) UNIQUE");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE code = ?", "x", "c1")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ?", "x", "t")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
      assertThat(database.countType("Comp", false)).isZero();

      database.transaction(() -> {
        database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ? AND code = ?", "x", "t", "c1");
        database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ? AND code = ?", "y", "t", "c1");
        // the equalities may come in any order, they only have to cover every property of the index
        database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE code = ? AND tenant = ?", "z", "c1", "t");
      });
      assertThat(database.countType("Comp", false)).isEqualTo(1);
    }

    @Test
    void upsertWithResidualPredicateStillUsesTheUniqueIndex() {
      database.command("sql", "CREATE DOCUMENT TYPE Resid");
      database.command("sql", "CREATE PROPERTY Resid.code STRING");
      database.command("sql", "CREATE INDEX ON Resid (code) UNIQUE");

      database.transaction(() -> {
        database.command("sql", "UPDATE Resid SET name = ? UPSERT WHERE code = ? AND name = ?", "n1", "c1", "n1");
        database.command("sql", "UPDATE Resid SET name = ? UPSERT WHERE code = ? AND name = ?", "n1", "c1", "n1");
      });
      assertThat(database.countType("Resid", false)).isEqualTo(1);
    }

    @Test
    void upsertWithRangeOrInConditionIsRejected() {
      database.command("sql", "CREATE DOCUMENT TYPE Rng");
      database.command("sql", "CREATE PROPERTY Rng.code STRING");
      database.command("sql", "CREATE INDEX ON Rng (code) UNIQUE");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Rng SET name = ? UPSERT WHERE code > ?", "x", "a")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Rng SET name = ? UPSERT WHERE code IN ['a','b']", "x")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
      assertThat(database.countType("Rng", false)).isZero();
    }

    @Test
    void upsertWithDuplicatedPredicateOnCompositeIndexIsRejected() {
      database.command("sql", "CREATE DOCUMENT TYPE Dup");
      database.command("sql", "CREATE PROPERTY Dup.tenant STRING");
      database.command("sql", "CREATE PROPERTY Dup.code STRING");
      database.command("sql", "CREATE INDEX ON Dup (tenant, code) UNIQUE");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Dup SET name = ? UPSERT WHERE tenant = ? AND tenant = ?", "x", "t", "t")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
      assertThat(database.countType("Dup", false)).isZero();
    }

    @Test
    void upsertWithOrConditionIsRejected() {
      database.command("sql", "CREATE DOCUMENT TYPE OrCond");
      database.command("sql", "CREATE PROPERTY OrCond.code STRING");
      database.command("sql", "CREATE INDEX ON OrCond (code) UNIQUE");

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE OrCond SET name = ? UPSERT WHERE code = ? OR code = ?", "x", "a", "b")))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
      assertThat(database.countType("OrCond", false)).isZero();
    }

    @Test
    void upsertOnUniqueFullKeyLookupUpdatesTheExistingRecord() {
      database.command("sql", "CREATE DOCUMENT TYPE Sub");
      database.command("sql", "CREATE PROPERTY Sub.code STRING");
      database.command("sql", "CREATE INDEX ON Sub (code) UNIQUE");
      database.transaction(() -> database.command("sql", "INSERT INTO Sub SET code = 'c1', name = 'old'"));

      database.transaction(() -> database.command("sql", "UPDATE Sub SET name = 'new' UPSERT WHERE code = 'c1'"));
      try (final ResultSet rs = database.query("sql", "SELECT name FROM Sub")) {
        assertThat(rs.next().<String>getProperty("name")).isEqualTo("new");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void upsertWithNonPlainKeyShapesIsRejected() {
      database.command("sql", "CREATE DOCUMENT TYPE Shapes");
      database.command("sql", "CREATE PROPERTY Shapes.code STRING");
      database.command("sql", "CREATE INDEX ON Shapes (code) UNIQUE");

      // reversed equality, null key, and a modified/wrapped left side are not a plain full-key lookup
      final String[] where = { "? = code", "code = ?", "code.toLowerCase() = ?", "code.length() = ?" };
      final Object[] value = { "c1", null, "c1", 2 };
      for (int k = 0; k < where.length; k++) {
        final String condition = where[k];
        final Object parameter = value[k];
        assertThatThrownBy(() -> database.transaction(
            () -> database.command("sql", "UPDATE Shapes SET name = ? UPSERT WHERE " + condition, "x", parameter)))
            .as(condition)
            .isInstanceOf(CommandSQLParsingException.class)
            .hasMessageContaining("UNIQUE index");
      }
      assertThat(database.countType("Shapes", false)).isZero();
    }

    @Test
    void upsertOnCompositeIndexWithResidualPredicate() {
      database.command("sql", "CREATE DOCUMENT TYPE CompResid");
      database.command("sql", "CREATE PROPERTY CompResid.tenant STRING");
      database.command("sql", "CREATE PROPERTY CompResid.code STRING");
      database.command("sql", "CREATE INDEX ON CompResid (tenant, code) UNIQUE");

      database.transaction(() -> {
        database.command("sql", "UPDATE CompResid SET name = ? UPSERT WHERE tenant = ? AND code = ? AND name = ?", "n", "t", "c1", "n");
        database.command("sql", "UPDATE CompResid SET name = ? UPSERT WHERE tenant = ? AND code = ? AND name = ?", "n", "t", "c1", "n");
      });
      assertThat(database.countType("CompResid", false)).isEqualTo(1);
    }
  }
}
