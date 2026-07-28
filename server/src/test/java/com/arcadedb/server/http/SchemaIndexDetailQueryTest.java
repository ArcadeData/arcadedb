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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/5469">issue #5469</a>: opening the "Indexes" tab in
 * Studio issued a {@code SELECT FROM schema:index:<name>} per index. For a compound index the auto-derived name is
 * {@code Type[propA,propB]}, and the comma inside it was not accepted by the lexer, so the server logged a SQL syntax error for
 * every compound index in the database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaIndexDetailQueryTest extends BaseGraphServerTest {

  @Test
  void compoundIndexDetailIsQueryableOverHttp() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("HttpCompoundIndexType");
      type.createProperty("propA", Type.STRING);
      type.createProperty("propB", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "propA", "propB");
    });

    final String indexName = "HttpCompoundIndexType[propA,propB]";

    // Exactly what Studio sends, both bare and back-tick quoted.
    for (final String target : new String[] { "schema:index:" + indexName, "schema:index:`" + indexName + "`" }) {
      final Response response = query("SELECT FROM " + target + " limit 20000");

      assertThat(response.status()).as(target + " -> " + response.body()).isEqualTo(200);
      final JSONArray result = new JSONObject(response.body()).getJSONArray("result");
      assertThat(result.length()).isEqualTo(1);
      assertThat(result.getJSONObject(0).getString("name")).isEqualTo(indexName);
    }

    // The listing must report every column the Studio table renders exactly as the per-index detail query does: that equivalence
    // is what makes the per-index query unnecessary to draw the table.
    final Response listing = query("SELECT FROM schema:indexes WHERE name = '" + indexName + "'");
    assertThat(listing.status()).isEqualTo(200);
    final JSONArray listedRows = new JSONObject(listing.body()).getJSONArray("result");
    assertThat(listedRows.length()).isEqualTo(1);
    final JSONObject listed = listedRows.getJSONObject(0);

    final JSONObject detail = new JSONObject(query("SELECT FROM schema:index:`" + indexName + "`").body())
        .getJSONArray("result").getJSONObject(0);

    // fileId/size are absent on a type-level index (they belong to its per-bucket sub-indexes): the point is that the listing and
    // the detail agree, so the table renders the same either way.
    for (final String column : new String[] { "name", "indexType", "typeName", "unique", "compacting", "valid", "fileId",
        "size" }) {
      assertThat(listed.has(column)).as("column '" + column + "'").isEqualTo(detail.has(column));
      if (detail.has(column))
        assertThat(listed.get(column)).as("column '" + column + "'").isEqualTo(detail.get(column));
    }
    assertThat(listed.getBoolean("valid")).isTrue();

    database.getSchema().dropType("HttpCompoundIndexType");
  }

  private Response query(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder(
            new URI("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/query/" + getDatabaseName()))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", "sql").put("command", command).toString(),
            StandardCharsets.UTF_8))
        .build();

    final HttpResponse<String> response = HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

    return new Response(response.statusCode(), response.body());
  }

  private record Response(int status, String body) {
  }
}
