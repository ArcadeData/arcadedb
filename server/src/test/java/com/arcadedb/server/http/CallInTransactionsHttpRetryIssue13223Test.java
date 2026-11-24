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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.graph.Vertex;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13223 over HTTP: the command handler runs a session-less request in an auto-commit transaction and replays it on a
 * conflict, up to the payload's {@code retries}. The batches of a {@code CALL { ... } IN TRANSACTIONS} are durable once they
 * commit, so a conflict on the final commit of the request replayed them: every batch was applied twice. The request now fails
 * with the conflict, and the batches it committed stand once.
 */
class CallInTransactionsHttpRetryIssue13223Test extends BaseGraphServerTest {

  @Test
  void aConflictAfterTheBatchesCommittedDoesNotReplayTheRequest() throws Exception {
    testEachServer(serverIndex -> {
      final String batchType = "Batch13223_" + serverIndex;
      final String keyType = "Key13223_" + serverIndex;
      final Database database = getServerDatabase(serverIndex, "graph");
      database.command("sql", "CREATE VERTEX TYPE " + batchType);
      database.command("sql", "CREATE VERTEX TYPE " + keyType);
      database.command("sql", "CREATE PROPERTY " + keyType + ".k INTEGER");
      database.command("sql", "CREATE INDEX ON " + keyType + " (k) UNIQUE");

      // While the first batch runs, another client commits the key the statement creates after the CALL: the request's own
      // commit then fails with a retryable duplicated key, which the handler used to answer by running the whole request again
      final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
      final AtomicInteger created = new AtomicInteger();
      final AfterRecordCreateListener concurrentInsert = record -> {
        if (record instanceof Vertex vertex && vertex.getTypeName().equals(batchType) && created.incrementAndGet() == 1) {
          final Thread other = new Thread(() -> {
            try {
              database.transaction(() -> database.newVertex(keyType).set("k", 1).save());
            } catch (final Throwable e) {
              concurrentFailure.set(e);
            }
          });
          other.start();
          try {
            other.join(30_000);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      };

      database.getEvents().registerListener(concurrentInsert);
      try {
        final JSONObject payload = new JSONObject();
        payload.put("language", "opencypher");
        payload.put("command", ("UNWIND range(1, 3) AS i CALL (i) { CREATE (:%s {i: i}) } IN TRANSACTIONS OF 1 ROW "
            + "WITH count(*) AS n CREATE (:%s {k: 1})").formatted(batchType, keyType));
        payload.put("retries", 3);

        final HttpURLConnection connection = post(serverIndex, payload);
        try {
          assertThat(connection.getResponseCode()).isNotEqualTo(200);
          assertThat(readError(connection)).contains("DuplicatedKeyException");
        } finally {
          connection.disconnect();
        }
      } finally {
        database.getEvents().unregisterListener(concurrentInsert);
      }

      assertThat(concurrentFailure.get()).isNull();
      // one attempt: a replay would have created the batches again
      assertThat(created.get()).isEqualTo(3);
      assertThat(count(database, batchType)).isEqualTo(3L);
      assertThat(count(database, keyType)).isEqualTo(1L);
    });
  }

  private static long count(final Database database, final String type) {
    return database.query("opencypher", "MATCH (x:" + type + ") RETURN count(x) AS c").next().<Number>getProperty("c").longValue();
  }

  private HttpURLConnection post(final int serverIndex, final JSONObject payload) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, "/api/v1/command/graph"))
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection, payload);
    connection.connect();
    return connection;
  }
}
