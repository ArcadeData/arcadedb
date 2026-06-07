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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
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
}
