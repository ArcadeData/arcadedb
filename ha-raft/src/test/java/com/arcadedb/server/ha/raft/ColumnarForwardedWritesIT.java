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
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes sent to a follower's HTTP command endpoint are forwarded to the leader, which commits them locally (its projection is fed by
 * the local commit path) and replicates them (the followers' projections are fed by the Raft apply); every node's facets end equal to
 * its GROUP BY (plan 3, Task 23).
 */
class ColumnarForwardedWritesIT extends ColumnarHATestSupport {
  private static final int        BATCHES    = 10;
  private static final int        BATCH_SIZE = 200;
  private static final HttpClient HTTP       = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @Test
  @Timeout(180)
  void writesSentToAFollowerAreProjectedEverywhere() throws Exception {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = database(leader);
    new ColumnarTestData(2304L).createSchema(leaderDb);
    assertClusterConsistency();
    final long leaderPagesBefore = manager(leader).getStats().applyPagesCount(ColumnarTestData.TYPE);

    for (int batch = 0; batch < BATCHES; batch++)
      postCommand(follower, "INSERT INTO " + ColumnarTestData.TYPE + " CONTENT " + batch(batch));
    postCommand(follower, "UPDATE " + ColumnarTestData.TYPE + " SET brand = 'brand-updated' WHERE fileSize < 102400");
    postCommand(follower, "UPDATE " + ColumnarTestData.TYPE + " SET tags = ['tag-updated'] WHERE fileSize >= 1024000");
    postCommand(follower, "DELETE FROM " + ColumnarTestData.TYPE + " WHERE brand = 'brand-7'");
    assertClusterConsistency();

    final long expected = survivors();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(database(i).countType(ColumnarTestData.TYPE, true)).as("server %d holds the forwarded writes", i).isEqualTo(expected);
    assertEquivalentEverywhere();
    assertThat(manager(leader).getStats().applyPagesCount(ColumnarTestData.TYPE))
        .as("the leader projected its local commits of the forwarded writes").isGreaterThan(leaderPagesBefore);
    assertThat(manager(follower).getStats().applyPagesCount(ColumnarTestData.TYPE))
        .as("the follower that received the writes projected them through its Raft apply").isPositive();
  }

  /** {@code BATCH_SIZE} records: 13 brands, up to 3 tags, a file size spread over 0 to 2 MB. */
  private static String batch(final int batch) {
    final JSONArray records = new JSONArray();
    for (int i = 0; i < BATCH_SIZE; i++) {
      final int n = batch * BATCH_SIZE + i;
      final JSONArray tags = new JSONArray();
      for (int t = 0; t < n % 4; t++)
        tags.put("tag-" + (n + t) % 37);
      records.put(new JSONObject().put("brand", "brand-" + n % 13).put("tags", tags).put("fileSize", 1024L * (n % 2048)));
    }
    return records.toString();
  }

  /** The records the DELETE keeps: every record but the brand-7 ones the first UPDATE did not rename. */
  private static long survivors() {
    long survivors = 0;
    for (int n = 0; n < BATCHES * BATCH_SIZE; n++)
      if (n % 13 != 7 || 1024L * (n % 2048) < 102_400)
        survivors++;
    return survivors;
  }

  private void postCommand(final int serverIndex, final String command) throws Exception {
    final String body = new JSONObject().put("language", "sql").put("command", command).toString();
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(serverIndex, "/api/v1/command/" + getDatabaseName())))
        .timeout(Duration.ofSeconds(60))
        .header("Content-Type", "application/json")
        .header("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
        .build();
    final HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("server %d answers '%s': %s", serverIndex, command, response.body()).isEqualTo(200);
  }
}
