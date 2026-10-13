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
package com.arcadedb.mcp;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9772: every refusal the MCP surface makes about the SHAPE of a request is answered in every server mode, wherever
 * the refusal is worded. Production mode conceals the engine's text (#8749), but a missing or malformed argument is text
 * about the request the caller needs to correct it. Only the refusals worded inside the {@code mcp} module carried the
 * marker class, so a bare {@code args.getString("query")} (a {@code JSONException}) and the shared search validators (a
 * plain {@code IllegalArgumentException}) were answered with the placeholder and logged as a SEVERE server fault.
 *
 * A one-element array is not used as a malformed shape here: the accessor unwraps it into its element (the dispatcher keeps that
 * coercion on purpose, see {@code MCPDispatcher#toolsCall}), so only an array of another size is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9772MCPMalformedArgumentsInProductionTest extends BaseGraphServerTest {

  @BeforeEach
  void enableMCP() throws Exception {
    post("/api/v1/mcp/config", new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("profile", "all")
        .put("allowedUsers", new JSONArray().put("root")));
  }

  @Test
  void queryWithoutQueryIsAnsweredInFull() throws Exception {
    assertRefused("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql"), "'query'");
  }

  @Test
  void queryWithoutDatabaseIsAnsweredInFull() throws Exception {
    assertRefused("query", new JSONObject().put("language", "sql").put("query", "SELECT 1"), "'database'");
  }

  @Test
  void queryAsAnArrayIsAnsweredInFull() throws Exception {
    assertRefused("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
        .put("query", new JSONArray().put("SELECT 1").put("SELECT 2")), "'query'");
  }

  @Test
  void queryWithANonNumericLimitIsAnsweredInFull() throws Exception {
    assertRefused("query", new JSONObject().put("database", getDatabaseName()).put("language", "sql")
        .put("query", "SELECT 1").put("limit", "abc"), "'limit'");
  }

  @Test
  void queryWithALanguageThatIsNotAStringIsAnsweredInFull() throws Exception {
    assertRefused("query", new JSONObject().put("database", getDatabaseName()).put("language", new JSONArray().put("sql").put("cypher"))
        .put("query", "SELECT 1"), "'language'");
  }

  @Test
  void executeCommandWithoutCommandIsAnsweredInFull() throws Exception {
    assertRefused("execute_command", new JSONObject().put("database", getDatabaseName()).put("language", "sql"), "'command'");
  }

  @Test
  void executeCommandWithADatabaseThatIsNotAStringIsAnsweredInFull() throws Exception {
    assertRefused("execute_command", new JSONObject().put("database", new JSONArray().put(getDatabaseName()).put("other"))
        .put("language", "sql").put("command", "SELECT 1"), "'database'");
  }

  @Test
  void getSchemaWithoutDatabaseIsAnsweredInFull() throws Exception {
    assertRefused("get_schema", new JSONObject(), "'database'");
  }

  @Test
  void fullTextSearchWithoutDatabaseIsAnsweredInFull() throws Exception {
    assertRefused("full_text_search", new JSONObject().put("indexName", "Person[name]").put("queryText", "a"), "'database'");
  }

  @Test
  void fullTextSearchWithABlankQueryTextIsAnsweredInFull() throws Exception {
    assertRefused("full_text_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Person[name]")
        .put("queryText", " "), "'queryText'");
  }

  @Test
  void fullTextSearchWithALimitOutOfRangeIsAnsweredInFull() throws Exception {
    assertRefused("full_text_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Person[name]")
        .put("queryText", "a").put("limit", 0), "'limit' must be between 1 and");
  }

  @Test
  void fullTextSearchWithANonNumericLimitIsAnsweredInFull() throws Exception {
    assertRefused("full_text_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Person[name]")
        .put("queryText", "a").put("limit", "abc"), "limit");
  }

  @Test
  void fullTextSearchWithNeitherAnIndexNameNorATypeNameIsAnsweredInFull() throws Exception {
    assertRefused("full_text_search", new JSONObject().put("database", getDatabaseName()).put("queryText", "a"),
        "Provide either 'indexName', or 'typeName'");
  }

  @Test
  void fullTextSearchWithPropertiesThatAreNotNamesIsAnsweredInFull() throws Exception {
    assertRefused("full_text_search", new JSONObject().put("database", getDatabaseName()).put("typeName", "Person")
        .put("queryText", "a").put("properties", new JSONArray().put(new JSONObject().put("x", 1))), "'properties'");
  }

  @Test
  void vectorSearchWithoutAQueryVectorIsAnsweredInFull() throws Exception {
    // the index exists in the fixture or not, the shape of the vector is reported first
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]"),
        "'queryVector' is required");
  }

  @Test
  void vectorSearchWithAQueryVectorThatIsNotNumbersIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put("a").put("b")), "'queryVector' must contain only numbers");
  }

  @Test
  void vectorSearchWithAnAllZeroDenseQueryVectorIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put(0.0f).put(0.0f)), "non-zero");
  }

  @Test
  void vectorSearchWithSparseIndicesOfTheWrongLengthIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]")
        .put("sparse", true).put("queryVector", new JSONArray().put(1.0f).put(2.0f))
        .put("queryIndices", new JSONArray().put(0)), "'queryIndices' and 'queryVector' must have the same length");
  }

  @Test
  void vectorSearchWithoutAnIndexNameIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName())
        .put("queryVector", new JSONArray().put(1.0f)), "'indexName'");
  }

  @Test
  void vectorSearchWithAnEfSearchOutOfRangeIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f)).put("efSearch", 0), "'efSearch' must be between 1 and");
  }

  @Test
  void vectorSearchWithQueryIndicesOnADenseSearchIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f)).put("queryIndices", new JSONArray().put(0)), "'queryIndices' requires sparse=true");
  }

  @Test
  void vectorSearchWithANonNumericKIsAnsweredInFull() throws Exception {
    assertRefused("vector_search", new JSONObject().put("database", getDatabaseName()).put("indexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f)).put("k", "abc"), "k");
  }

  @Test
  void hybridSearchWithAKOutOfRangeIsAnsweredInFull() throws Exception {
    assertRefused("hybrid_search", new JSONObject().put("database", getDatabaseName()).put("vectorIndexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f)).put("k", 0), "'k' must be between 1 and");
  }

  @Test
  void sampleRecordsWithANonNumericLimitIsAnsweredInFull() throws Exception {
    assertRefused("sample_records", new JSONObject().put("database", getDatabaseName()).put("limit", "abc"), "'limit'");
  }

  @Test
  void sampleRecordsWithTypesThatIsNotAnArrayIsAnsweredInFull() throws Exception {
    assertRefused("sample_records", new JSONObject().put("database", getDatabaseName()).put("types", "V1"), "'types'");
  }

  @Test
  void sampleRecordsWithATypesElementThatIsNotANameIsAnsweredInFull() throws Exception {
    assertRefused("sample_records", new JSONObject().put("database", getDatabaseName())
        .put("types", new JSONArray().put((Object) null)), "'types'");
    assertRefused("sample_records", new JSONObject().put("database", getDatabaseName())
        .put("types", new JSONArray().put(new JSONObject())), "'types'");
  }

  @Test
  void upsertEntityWithSetPropertiesThatIsNotAnObjectIsAnsweredInFull() throws Exception {
    assertRefused("upsert_entity", new JSONObject().put("database", getDatabaseName()).put("typeName", "V1")
        .put("matchKeys", new JSONObject().put("id", 1)).put("setProperties", new JSONArray().put("x")), "'setProperties'");
  }

  /** The engine's own text is still concealed: only the refusals worded about the request are kept. */
  @Test
  void engineTextIsStillConcealedInProductionMode() throws Exception {
    final JSONObject response = inProduction(() -> callTool("query", new JSONObject().put("database", getDatabaseName())
        .put("language", "sql").put("query", "SELECT FROM TypeThatDoesNotExist9772")));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(textOf(response)).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  private void assertRefused(final String tool, final JSONObject arguments, final String expectedFragment) throws Exception {
    final JSONObject response = inProduction(() -> callTool(tool, arguments));

    assertThat(response.getBoolean("isError", false)).as("%s %s must be refused", tool, arguments).isTrue();
    assertThat(textOf(response)).as("%s %s must be answered with its own wording, not the concealment placeholder", tool, arguments)
        .isNotEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE).contains(expectedFragment);
  }

  private static String textOf(final JSONObject response) {
    return response.getJSONArray("content").getJSONObject(0).getString("text");
  }

  private JSONObject inProduction(final ThrowingSupplier work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, "production");
    try {
      return work.get();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }

  private JSONObject callTool(final String toolName, final JSONObject arguments) throws Exception {
    final JSONObject response = post("/api/v1/mcp", new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 10)
        .put("method", "tools/call")
        .put("params", new JSONObject().put("name", toolName).put("arguments", arguments)));
    assertThat(response.has("result")).isTrue();
    return response.getJSONObject("result");
  }

  private JSONObject post(final String path, final JSONObject payload) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), "utf8"));
    } finally {
      connection.disconnect();
    }
  }

  @FunctionalInterface
  private interface ThrowingSupplier {
    JSONObject get() throws Exception;
  }
}
