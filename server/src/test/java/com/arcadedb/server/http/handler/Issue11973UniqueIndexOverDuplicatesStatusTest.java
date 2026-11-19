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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11973: an openCypher unique constraint built over values that are not unique fails with a {@link DuplicatedKeyException}
 * under two index build wrappers and the statement's own, and the HTTP classifier looked at the first two links only, so the
 * caller's data conflict answered 500. It is the 409 every other duplicated key answers.
 */
class Issue11973UniqueIndexOverDuplicatesStatusTest extends BaseGraphServerTest {

  @BeforeEach
  void createDuplicates() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue11973D {x: 1}), (:Issue11973D {x: 1})"));
  }

  @Test
  void aCypherUniqueConstraintOverDuplicatesIs409() throws Exception {
    assertConflict("opencypher", "CREATE CONSTRAINT c11973 FOR (n:Issue11973D) REQUIRE n.x IS UNIQUE");
  }

  private void assertConflict(final String language, final String statement) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection, language, statement, null, Map.of());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).as(statement).isEqualTo(409);
      final String body;
      try (final InputStream in = connection.getErrorStream()) {
        body = FileUtils.readStreamAsString(in, "UTF-8");
      }
      final JSONObject answer = new JSONObject(body);
      assertThat(answer.getString("exception", "")).as(statement).isEqualTo(DuplicatedKeyException.class.getName());
      assertThat(answer.getString("detail", "")).as(statement).contains("Duplicated key");
    } finally {
      connection.disconnect();
    }
    // nothing of the refused index is left
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType("Issue11973D").getAllIndexes(false)).isEmpty();
  }
}
