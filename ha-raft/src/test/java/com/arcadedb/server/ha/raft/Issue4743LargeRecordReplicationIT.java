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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4743: with the STOCK configuration, a single record larger than the old 4MB
 * {@code arcadedb.ha.appendBufferSize} could not be replicated at all - and trying made the Ratis leader
 * step down, after which the retry toppled its successor, so the cluster churned elections forever
 * without the write ever landing.
 * <p>
 * The limit applies to the COMPRESSED WAL of the transaction, so the payload here is deliberately
 * high-entropy: repetitive text collapses ~200x (an 8MB JSON-ish record encodes to ~45KB) and would sail
 * under even the old default, proving nothing. Incompressible content - binary blobs, base64, encrypted
 * fields, float vectors - maps roughly 1:1, and that is the shape that used to break the cluster.
 * <p>
 * This test deliberately does NOT override the buffer settings: its whole purpose is to pin the
 * out-of-the-box behaviour.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4743LargeRecordReplicationIT extends BaseRaftHATest {

  /** Above the old 4MB ceiling, and in the middle of the reporter's stated 4-6.5MB record range. */
  private static final int LARGE_RECORD_BYTES = 6 * 1024 * 1024;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void populateDatabase() {
  }

  @Test
  @Tag("slow")
  void recordLargerThanTheOldCeilingReplicatesToEveryNode() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final long termBefore = getRaftPlugin(leaderIndex).getRaftHAServer().getCurrentTerm();

    final Database database = getServerDatabase(leaderIndex, getDatabaseName());
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Blob");
      type.createProperty("name", String.class);
      type.createProperty("payload", String.class);
    });

    final String payload = incompressible(LARGE_RECORD_BYTES);

    // One transaction, one record, ~6MB of incompressible content. On the old 4MB default this is where
    // the leader stepped down and the write never landed.
    database.transaction(() -> database.newDocument("Blob").set("name", "single").set("payload", payload).save());

    // And a transaction carrying several of them at once, since the ceiling applies per TRANSACTION, not
    // per record: this is the shape a GraphBatch produces.
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Blob").set("name", "batch-" + i).set("payload", payload).save();
    });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    testEachServer(serverIndex -> {
      final Database serverDb = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(serverDb.countType("Blob", false))
          .as("every large record must replicate to server %d", serverIndex).isEqualTo(4);
      try (final ResultSet rs = serverDb.query("sql", "SELECT payload FROM Blob WHERE name = 'single'")) {
        assertThat(rs.hasNext()).as("the large record must be readable on server %d", serverIndex).isTrue();
        assertThat(rs.next().<String>getProperty("payload"))
            .as("the large record must round-trip byte for byte on server %d", serverIndex)
            .isEqualTo(payload);
      }
    });

    assertThat(getRaftPlugin(leaderIndex).getRaftHAServer().getCurrentTerm())
        .as("replicating a large record must not make the leader step down").isEqualTo(termBefore);
  }

  /**
   * Fixed-seed high-entropy ASCII so the WAL barely compresses: the resulting Raft entry tracks the record
   * size (a 6MB payload measures ~6.03MB encoded).
   */
  private static String incompressible(final int size) {
    final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    final Random rnd = new Random(4743);
    final char[] chars = new char[size];
    for (int i = 0; i < size; i++)
      chars[i] = alphabet.charAt(rnd.nextInt(alphabet.length()));
    return new String(chars);
  }
}
