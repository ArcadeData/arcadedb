/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4144: in an HA-wrapped database, updating a previously-persisted
 * record on the leader threw {@code ClassCastException: RaftReplicatedDatabase cannot be cast to
 * LocalDatabase} from {@code DocumentValidator} when the type had a readonly property.
 * <p>
 * The fix routes through {@code DatabaseInternal.getEmbedded()} so the wrapper is unwrapped
 * to the underlying {@code LocalDatabase} before calling the LocalDatabase-specific
 * {@code getOriginalDocument} method.
 */
class RaftReadonlyPropertyIssue4144IT extends BaseRaftHATest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void readonlyPropertyValidationDoesNotClassCastUnderHA() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final var db = getServerDatabase(leaderIndex, getDatabaseName());

    // Create a type with a readonly property and a mutable property. The validator only
    // walks readonly properties when the document is dirty AND already has an identity, so
    // we need a real persisted record that we then modify.
    db.transaction(() -> {
      if (!db.getSchema().existsType("Issue4144"))
        db.getSchema().createDocumentType("Issue4144")
            .createProperty("id", Type.STRING).setReadonly(true);
      db.getSchema().getType("Issue4144").createProperty("payload", Type.STRING);
    });

    // Insert one record so we have something to update.
    final Object[] ridHolder = new Object[1];
    db.transaction(() -> {
      final MutableDocument doc = db.newDocument("Issue4144");
      doc.set("id", "k-1");
      doc.set("payload", "v1");
      doc.save();
      ridHolder[0] = doc.getIdentity();
    });

    // Update the mutable property on the persisted record. Pre-fix this triggered the validator's
    // (LocalDatabase) cast on the wrapper and crashed with a ClassCastException; post-fix the
    // unwrap reaches the embedded LocalDatabase and the update succeeds.
    db.transaction(() -> {
      final MutableDocument loaded = db.lookupByRID((com.arcadedb.database.RID) ridHolder[0], true).asDocument().modify();
      loaded.set("payload", "v2");
      loaded.save();
    });

    final MutableDocument afterUpdate = db.lookupByRID((com.arcadedb.database.RID) ridHolder[0], true).asDocument().modify();
    assertThat(afterUpdate.getString("payload")).isEqualTo("v2");
    assertThat(afterUpdate.getString("id")).isEqualTo("k-1");
  }

  @Test
  void readonlyPropertyChangeStillRejectedUnderHA() {
    // Sanity check: the unwrap fix must NOT change the validation behavior. Trying to mutate
    // the readonly property itself should still throw ValidationException, not silently succeed.
    final int leaderIndex = findLeaderIndex();
    final var db = getServerDatabase(leaderIndex, getDatabaseName());

    db.transaction(() -> {
      if (!db.getSchema().existsType("Issue4144Strict"))
        db.getSchema().createDocumentType("Issue4144Strict")
            .createProperty("id", Type.STRING).setReadonly(true);
    });

    final Object[] ridHolder = new Object[1];
    db.transaction(() -> {
      final MutableDocument doc = db.newDocument("Issue4144Strict");
      doc.set("id", "original");
      doc.save();
      ridHolder[0] = doc.getIdentity();
    });

    assertThatThrownBy(() -> db.transaction(() -> {
      final MutableDocument loaded = db.lookupByRID((com.arcadedb.database.RID) ridHolder[0], true).asDocument().modify();
      loaded.set("id", "changed");
      loaded.save();
    }))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("immutable");
  }
}
