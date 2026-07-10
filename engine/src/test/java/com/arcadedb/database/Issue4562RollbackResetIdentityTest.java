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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #4562 follow-up: a record created inside a transaction that is later rolled back (explicitly or by a failed
 * commit) must have its optimistically-assigned identity reset to provisional, so re-saving the same in-memory object
 * cleanly INSERTs a new record instead of being treated as an update of a now-missing record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4562RollbackResetIdentityTest extends TestHelper {

  private static final String TYPE = "SimpleVertexEx";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType(TYPE);
      type.createProperty("svex", String.class);
      type.createProperty("svuuid", String.class);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "svuuid");
    });
  }

  @Test
  void explicitRollbackResetsCreatedRecordIdentity() {
    database.begin();
    final MutableVertex v = database.newVertex(TYPE);
    v.set("svex", "a");
    v.set("svuuid", "u1");
    v.save();
    assertThat(v.getIdentity()).as("record gets an optimistic RID at creation").isNotNull();
    database.rollback();

    assertThat(v.getIdentity()).as("identity must be reset to provisional after rollback").isNull();

    // re-saving the same in-memory object must cleanly INSERT a new record
    database.begin();
    v.save();
    final RID newRid = v.getIdentity();
    database.commit();

    assertThat(newRid).as("a fresh RID is assigned on re-insert").isNotNull();
    assertThat(database.countType(TYPE, false)).isEqualTo(1);
  }

  @Test
  void failedCommitResetsIdentityOfRecordsCreatedInThatTx() {
    // seed: v1 owns uuid u1
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(TYPE);
      v1.set("svex", "v1");
      v1.set("svuuid", "u1");
      v1.save();
    });

    // tx that fails on commit: v2 duplicates u1, v3 is a fresh record
    database.begin();
    final MutableVertex v2 = database.newVertex(TYPE);
    v2.set("svex", "v2");
    v2.set("svuuid", "u1"); // duplicate -> commit will fail
    v2.save();

    final MutableVertex v3 = database.newVertex(TYPE);
    v3.set("svex", "v3");
    v3.set("svuuid", "u3");
    v3.save();
    assertThat(v3.getIdentity()).isNotNull();

    final Throwable error = catchThrowable(() -> database.commit());
    assertThat(error).isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.isTransactionActive()).isFalse();

    // both records created in the rolled-back tx must be provisional again
    assertThat(v2.getIdentity()).as("v2 identity reset after rollback").isNull();
    assertThat(v3.getIdentity()).as("v3 identity reset after rollback").isNull();

    // re-inserting v3 (fixing nothing) must work cleanly
    database.begin();
    v3.save();
    database.commit();

    assertThat(database.countType(TYPE, false)).as("v1 + re-inserted v3").isEqualTo(2);
  }

  @Test
  void successfulCommitDoesNotResetIdentity() {
    database.begin();
    final MutableVertex v = database.newVertex(TYPE);
    v.set("svex", "a");
    v.set("svuuid", "u1");
    v.save();
    database.commit();

    assertThat(v.getIdentity()).as("identity must survive a successful commit").isNotNull();
  }

  @Test
  void committedRecordKeepsIdentityEvenIfLaterDeleted() {
    // The new rollback reset must only affect records created in a transaction that ROLLED BACK. A record committed in
    // an earlier transaction must keep its identity, even if a later transaction (that also commits) deletes it.
    final MutableVertex[] vHolder = new MutableVertex[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex(TYPE);
      v.set("svex", "ghost");
      v.set("svuuid", "g1");
      v.save();
      vHolder[0] = v;
    });

    final RID committedRid = vHolder[0].getIdentity();
    assertThat(committedRid).isNotNull();

    database.transaction(() -> database.deleteRecord(database.lookupByRID(committedRid, false)));

    assertThat(vHolder[0].getIdentity()).as("a committed record's identity is never reset by a later tx").isEqualTo(committedRid);
  }
}
