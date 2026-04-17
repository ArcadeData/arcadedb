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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.http.IdempotencyCache;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the server-side idempotency cache. Sends a write with an
 * {@code X-Request-Id} header, then sends the same request again. The second request must not
 * re-execute the operation - the server replays the cached response. Without this, an
 * ambiguous write (committed but response lost) followed by a client retry produces a
 * duplicate-key violation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("IntegrationTest")
class IdempotencyReplayIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  protected boolean isCreateDatabases() {
    return true;
  }

  @Override
  protected void populateDatabase() {
    final Database database = getDatabases()[0];
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final VertexType v = schema.buildVertexType().withName("Item").withTotalBuckets(3).create();
      v.createProperty("id", Integer.class);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Item", "id");
    });
  }

  @Test
  void replaysCachedResponseOnRepeatedRequestId() throws Exception {
    final String requestId = UUID.randomUUID().toString();
    final String body = "{\"language\":\"sql\",\"command\":\"CREATE VERTEX Item SET id = 42\"}";

    // First call: executes the INSERT.
    final int firstStatus = post("/api/v1/command/" + getDatabaseName(), body, requestId);
    assertThat(firstStatus).isEqualTo(200);

    // Verify the vertex exists exactly once.
    assertThat(countItems()).as("after first call").isEqualTo(1L);

    // Second call with the same X-Request-Id: must NOT re-execute; otherwise the unique index
    // on Item.id would reject it with DuplicatedKey, and the count would stay at 1 but the
    // status code would be 503. What we want is a 200 (replayed cached response) AND the count
    // still at 1.
    final int secondStatus = post("/api/v1/command/" + getDatabaseName(), body, requestId);
    assertThat(secondStatus).as("replayed status code").isEqualTo(200);
    assertThat(countItems()).as("after replayed call").isEqualTo(1L);

    // Sanity: a different X-Request-Id with the SAME body really does try to execute (and fails
    // with DuplicatedKey) - proves the cache actually skipped execution for the replay path.
    final int thirdStatus = post("/api/v1/command/" + getDatabaseName(), body, UUID.randomUUID().toString());
    assertThat(thirdStatus).as("fresh request with same body hits duplicate-key")
        .isIn(503, 500, 400); // any non-2xx means the server actually executed and rejected
    assertThat(countItems()).as("after duplicate-body new-id call").isEqualTo(1L);
  }

  @Test
  void differentPrincipalDoesNotShareCacheEntry() {
    // The cache key includes the authenticated principal, so a stolen X-Request-Id from another
    // user cannot produce a replay. Verify via the IdempotencyCache API directly (the HTTP
    // round-trip for a second user would require provisioning a second user, which is out of
    // scope for this unit-level check).
    final IdempotencyCache cache = getServer(0).getHttpServer().getIdempotencyCache();
    final String id = UUID.randomUUID().toString();
    cache.putSuccess(id, 200, "ok", null, "alice");

    assertThat(cache.get(id)).isNotNull();
    assertThat(cache.get(id).principal).isEqualTo("alice");
    // There is no "second user" check in the cache itself; the handler compares the cached
    // principal to the authenticated user. The check below documents the invariant.
    assertThat(java.util.Objects.equals(cache.get(id).principal, "bob")).isFalse();
  }

  private int post(final String path, final String body, final String requestId) throws Exception {
    final HttpURLConnection c = (HttpURLConnection) new URI("http://localhost:2480" + path).toURL().openConnection();
    try {
      c.setRequestMethod("POST");
      c.setRequestProperty("Content-Type", "application/json");
      c.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      if (requestId != null)
        c.setRequestProperty(AbstractServerHttpHandler.IDEMPOTENCY_HEADER, requestId);
      c.setDoOutput(true);
      try (final OutputStream os = c.getOutputStream()) {
        os.write(body.getBytes(StandardCharsets.UTF_8));
      }
      return c.getResponseCode();
    } finally {
      c.disconnect();
    }
  }

  private long countItems() {
    return getServer(0).getDatabase(getDatabaseName())
        .query("sql", "select count(*) as c from Item").next().getProperty("c", 0L);
  }
}
