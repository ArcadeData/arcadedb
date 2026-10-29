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

import com.arcadedb.database.Database;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.info.SchemaInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10928: whether an MCP tool call is listed by {@code list queries} and can be terminated used to be decided by the
 * same branch that decides whether it waits for a query admission slot, so {@code get_schema} - which skips the gate to stay
 * answerable on a busy server, but opens a database and walks its whole schema - did work no operator could see or stop. The
 * two decisions are now separate: every tool that runs in a database is listed, only the ones that run queries are admitted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10928MCPSchemaReadListedTest extends BaseGraphServerTest {
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  /**
   * The rule is the tool's own definition: a tool that takes a {@code database} argument runs in that database and is
   * listed. Derived from the definitions rather than enumerated, so a new database tool cannot be registered without one.
   */
  @Test
  void everyToolThatRunsInADatabaseIsListedAndEveryAdmittedToolIsListed() {
    for (final String tool : MCPDispatcher.REGISTERED_TOOL_NAMES)
      if (MCPDispatcher.isAdmittedTool(tool))
        assertThat(MCPDispatcher.isListedTool(tool)).as("admitted tool %s must also be listed", tool).isTrue();

    for (final String databaseTool : List.of("get_schema", "query", "execute_command", "sample_records", "vector_search",
        "full_text_search", "hybrid_search", "upsert_entity", "upsert_relationship"))
      assertThat(MCPDispatcher.isListedTool(databaseTool)).as(databaseTool).isTrue();
    assertThat(MCPDispatcher.isAdmittedTool("get_schema")).as("the schema stays answerable on a busy server").isFalse();
    for (final String serverTool : List.of("list_databases", "server_status", "profiler_start", "profiler_stop", "profiler_status",
        "get_server_settings", "set_server_setting"))
      assertThat(MCPDispatcher.isListedTool(serverTool)).as(serverTool).isFalse();
  }

  /** The entry is only worth having if a terminate reaches the work: the schema walk checks it between types. */
  @Test
  void aSchemaReadStopsWhenItsEntryIsTerminated() {
    try (final RunningQuery entry = getServer(0).getRunningQueries()
        .register(getDatabaseName(), "root", MCPDispatcher.MCP_PROTOCOL, null, null, RunningQuery.Client.NONE)) {
      entry.terminate("root");
      assertThatThrownBy(() -> SchemaInfo.toJSON(getServer(0).getDatabaseForOperator(getDatabaseName()), getDatabaseName()))
          .isInstanceOf(QueryTerminatedException.class);
    }
    // nothing registered: the walk runs as it always did
    assertThat(SchemaInfo.toJSON(getServer(0).getDatabaseForOperator(getDatabaseName()), getDatabaseName()).getJSONArray("types")
        .length()).isGreaterThan(0);
  }

  /**
   * A terminate that arrives once the walk has started stops it at the next type, not only one that was there before it
   * began: the real database and schema are seen through proxies whose type collection terminates the entry when the walk
   * fetches its second type, so a check hoisted in front of the loop would let the walk run to the end and fail this.
   */
  @Test
  void aSchemaReadTerminatedMidWalkStopsAtTheNextType() {
    final Database real = getServer(0).getDatabaseForOperator(getDatabaseName());
    final List<DocumentType> types = new ArrayList<>(real.getSchema().getTypes());
    assertThat(types.size()).as("the fixture needs at least two types").isGreaterThanOrEqualTo(2);

    final AtomicInteger fetched = new AtomicInteger();
    try (final RunningQuery entry = getServer(0).getRunningQueries()
        .register(getDatabaseName(), "root", MCPDispatcher.MCP_PROTOCOL, null, null, RunningQuery.Client.NONE)) {
      final Collection<DocumentType> terminatingTypes = new AbstractCollection<>() {
        @Override
        public Iterator<DocumentType> iterator() {
          final Iterator<DocumentType> delegate = types.iterator();
          return new Iterator<>() {
            @Override
            public boolean hasNext() {
              return delegate.hasNext();
            }

            @Override
            public DocumentType next() {
              if (fetched.incrementAndGet() == 2)
                entry.terminate("root");
              return delegate.next();
            }
          };
        }

        @Override
        public int size() {
          return types.size();
        }
      };
      final Schema schema = proxy(Schema.class, real.getSchema(), "getTypes", terminatingTypes);
      final Database database = proxy(Database.class, real, "getSchema", schema);

      assertThatThrownBy(() -> SchemaInfo.toJSON(database, getDatabaseName())).isInstanceOf(QueryTerminatedException.class);
    }
    assertThat(fetched.get()).as("the first type was rendered, the walk stopped at the second").isEqualTo(2);
  }

  /** A view of {@code target} whose no-argument {@code method} answers {@code answer}; every other call goes to the target. */
  @SuppressWarnings("unchecked")
  private static <T> T proxy(final Class<T> type, final T target, final String method, final Object answer) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (self, invoked, args) -> {
      if (method.equals(invoked.getName()) && invoked.getParameterCount() == 0)
        return answer;
      try {
        return invoked.invoke(target, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    });
  }

  /**
   * End to end: ids are handed out in sequence, so the gap between two probe entries opened around a call says whether the
   * call opened one of its own. Deterministic, where catching a millisecond schema read in a listing would not be.
   */
  @Test
  void getSchemaAndTheSchemaResourceOpenAnEntryWhileServerToolsDoNot() {
    final MCPConfiguration mcp = MCPPlugin.of(getServer(0)).getConfiguration();
    final boolean savedEnabled = mcp.isEnabled();
    final List<String> savedUsers = mcp.getAllowedUsers();
    try {
      mcp.setEnabled(true);
      mcp.setAllowedUsers(List.of("root"));

      final JSONObject toolCall = new JSONObject().put("name", "get_schema")
          .put("arguments", new JSONObject().put("database", getDatabaseName()));
      assertThat(entriesOpenedBy("tools/call", toolCall)).as("get_schema").isEqualTo(1);

      final JSONObject resourceRead = new JSONObject().put("uri", MCPResources.schemaURI(getDatabaseName()));
      assertThat(entriesOpenedBy("resources/read", resourceRead)).as("resources/read of a schema").isEqualTo(1);

      assertThat(entriesOpenedBy("tools/call", new JSONObject().put("name", "list_databases").put("arguments", new JSONObject())))
          .as("list_databases").isZero();
      assertThat(entriesOpenedBy("tools/call", new JSONObject().put("name", "server_status").put("arguments", new JSONObject())))
          .as("server_status").isZero();

      // every entry left the list with its call
      assertThat(getServer(0).getRunningQueries().getRunning()).noneMatch(q -> MCPDispatcher.MCP_PROTOCOL.equals(q.getProtocol()));
    } finally {
      mcp.setEnabled(savedEnabled);
      mcp.setAllowedUsers(savedUsers);
    }
  }

  private long entriesOpenedBy(final String method, final JSONObject params) {
    final long before = probeId();
    final JSONObject response = send(method, params);
    assertThat(response.has("error")).as(response.toString()).isFalse();
    assertThat(response.getJSONObject("result").getBoolean("isError", false)).as(response.toString()).isFalse();
    return probeId() - before - 1;
  }

  private long probeId() {
    try (final RunningQuery probe = getServer(0).getRunningQueries()
        .open(null, "probe", "test", null, null, RunningQuery.Client.NONE)) {
      final String id = probe.getId();
      return Long.parseLong(id.substring(1, id.lastIndexOf('-')));
    }
  }

  private JSONObject send(final String method, final JSONObject params) {
    try {
      final JSONObject body = new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", method).put("params", params);
      final HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/mcp")))
          .header("Content-Type", "application/json")
          .header("Authorization",
              "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
          .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
      return new JSONObject(response.body());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
