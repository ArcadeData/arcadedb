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
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10051 (#10018): a transaction committed inside a schema session on the leader does not travel in a {@code TX_ENTRY}: its
 * WAL is buffered and ships inside the {@code SCHEMA_ENTRY} the session sends on its way out, which carries no corpus deltas. A
 * follower would keep the token-length total of a document such a transaction rewrote, and a document count cannot tell. The
 * transaction therefore marks the counters of the indexes it wrote as recomputed (a newer epoch), so a follower adopts the
 * counters the schema entry carries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10051SchemaSessionCommitMarksCountersTest {
  private static final String TYPE  = "Doc10051S";
  private static final String PLAIN = "Plain10051S";
  private static final String INDEX = TYPE + "[content]";

  @TempDir
  Path tempDir;

  private LocalDatabase                            proxied;
  private FakeRaftTransactionBroker                broker;
  private RaftReplicatedDatabase                   database;
  private ThreadLocal<Boolean>                     schemaCommitThread;
  private ThreadLocal<List<byte[]>>                schemaWalBuffer;
  private ThreadLocal<List<Map<Integer, Integer>>> schemaBucketDeltaBuffer;

  @BeforeEach
  void setUp() throws Exception {
    // Every piece of schema and every property name is put in place BEFORE the wrapper is installed
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue10051s").toString()).create();
    proxied.transaction(() -> {
      proxied.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
      proxied.command("sql", "CREATE PROPERTY " + TYPE + ".content STRING");
      proxied.command("sql", "CREATE INDEX ON " + TYPE + " (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      proxied.command("sql", "CREATE DOCUMENT TYPE " + PLAIN);
      proxied.command("sql", "CREATE PROPERTY " + PLAIN + ".name STRING");
    });
    proxied.transaction(() -> {
      for (int i = 0; i < 4; i++)
        proxied.command("sql", "INSERT INTO " + TYPE + " SET content = 'alpha beta gamma'");
      proxied.command("sql", "INSERT INTO " + PLAIN + " SET name = 'seed'");
    });

    broker = new FakeRaftTransactionBroker();
    database = new RaftReplicatedDatabase(null, proxied, FakeRaftHAServer.detached().leader(true).transactionBroker(broker));

    schemaCommitThread = threadLocal("isSchemaCommitThread");
    schemaWalBuffer = threadLocal("schemaWalBuffer");
    schemaBucketDeltaBuffer = threadLocal("schemaBucketDeltaBuffer");
  }

  @AfterEach
  void tearDown() {
    schemaCommitThread.remove();
    schemaWalBuffer.remove();
    schemaBucketDeltaBuffer.remove();
    if (proxied != null && proxied.isOpen()) {
      proxied.rollbackAllNested();
      proxied.drop();
    }
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(proxied.getSchema().getIndexByName(INDEX));
  }

  @Test
  void aTransactionOfASchemaSessionThatWroteAFullTextIndexMarksItsCountersForTheFollowers() {
    assertThat(metadata().getCountersEpoch()).isZero();

    database.begin();
    // a rewrite that keeps the document count: only the token-length total can tell it happened
    proxied.iterateType(TYPE, false).next().asDocument().modify().set("content", "alpha beta gamma delta epsilon").save();
    schemaCommitThread.set(Boolean.TRUE);
    database.commit();

    assertThat(schemaWalBuffer.get()).as("the WAL waits for the SCHEMA_ENTRY").hasSize(1);
    assertThat(broker.calls("replicateTransaction")).as("and no TX_ENTRY carries it").isEmpty();
    assertThat(metadata().getTotalDocs()).isEqualTo(4L);
    assertThat(metadata().getSumDocLength()).as("the leader counted the rewrite").isEqualTo(4 * 3L + 2);
    assertThat(metadata().getCountersEpoch()).as("and marked the counters the schema entry will carry").isEqualTo(1L);
  }

  @Test
  void aTransactionOfASchemaSessionThatWroteNoFullTextIndexLeavesTheEpochAlone() {
    database.begin();
    proxied.iterateType(PLAIN, false).next().asDocument().modify().set("name", "changed").save();
    schemaCommitThread.set(Boolean.TRUE);
    database.commit();

    assertThat(schemaWalBuffer.get()).hasSize(1);
    assertThat(metadata().getCountersEpoch()).isZero();
  }

  @SuppressWarnings("unchecked")
  private static <T> ThreadLocal<T> threadLocal(final String name) throws Exception {
    final Field field = RaftReplicatedDatabase.class.getDeclaredField(name);
    field.setAccessible(true);
    return (ThreadLocal<T>) field.get(null);
  }
}
