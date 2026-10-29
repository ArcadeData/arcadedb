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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10856: a float or double array is written in its fixed-width form, which a build that predates it cannot read. On
 * a replicated database the record bytes reach every peer in the pages a transaction ships, so no node writes the form
 * until every peer has advertised {@link PeerCapabilities#FIXED_WIDTH_FLOAT_ARRAYS}; until then the varint form, which
 * every build reads, is written. A peer that joins invalidates the answer at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10856FixedWidthFloatArrayCapabilityTest {
  private static final int DIM = 256;

  private final String           path = "target/databases/issue10856-fixed-width-capability";
  private       LocalDatabase    database;
  private       FakeRaftHAServer raft;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(path));
    database = (LocalDatabase) new DatabaseFactory(path).create();
    database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
    raft = FakeRaftHAServer.detached();
    new RaftReplicatedDatabase(null, database, raft);
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void thisBuildAdvertisesTheForm() {
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS);
  }

  @Test
  void theVarintFormIsWrittenWhileAPeerHasNotAdvertisedTheFixedOne() {
    raft.returns("peersMissingCapability", List.of("old-peer"));

    assertThat(serializedSize()).as("5 bytes per float: the form every build reads").isGreaterThanOrEqualTo(5 * DIM);
    assertThat(raft.calls("peersMissingCapability")).extracting(call -> call.getFirst())
        .contains(PeerCapabilities.FIXED_WIDTH_FLOAT_ARRAYS);
  }

  @Test
  void theFixedFormIsWrittenOnceEveryPeerAdvertisesIt() {
    raft.returns("peersMissingCapability", List.of());

    assertThat(serializedSize()).isBetween(4 * DIM, 4 * DIM + 32);
  }

  /** The cached verdict is dropped when the membership changes: a peer that just joined has not answered yet. */
  @Test
  void aMembershipChangeDropsTheCachedVerdict() {
    raft.returns("peersMissingCapability", List.of());
    assertThat(serializedSize()).isLessThan(5 * DIM);

    raft.returns("peersMissingCapability", List.of("joined-peer"));
    assertThat(serializedSize()).as("still the cached verdict, within its second").isLessThan(5 * DIM);

    raft.invalidateFixedWidthFloatArraysCapability();
    assertThat(serializedSize()).isGreaterThanOrEqualTo(5 * DIM);
  }

  private int serializedSize() {
    final float[] vector = new float[DIM];
    for (int i = 0; i < DIM; i++)
      vector[i] = (float) Math.sin(i + 1) / 16;
    final MutableDocument doc = database.newDocument("Doc").set("embedding", vector);
    return database.getSerializer().serialize(database, doc).size();
  }
}
