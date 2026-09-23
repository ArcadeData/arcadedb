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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;

/**
 * {@link NodeReader} over each node's own {@code /api/v1/query} endpoint, paging by key so a soak with millions of
 * rows never loads more than one page per request.
 */
public final class HttpNodeReader implements NodeReader {
  private static final int CONNECT_TIMEOUT_MS = 5_000;
  private static final int READ_TIMEOUT_MS    = 120_000;

  /** The node answered with a 5xx: it is reachable but could not serve its own data. */
  public static final class ServerErrorException extends IOException {
    private final int status;

    public ServerErrorException(final int node, final int status, final String body) {
      super("node " + node + " query failed with HTTP " + status + ": " + body);
      this.status = status;
    }

    public int status() {
      return status;
    }
  }

  interface PageFetcher {
    JSONArray fetch(long last) throws IOException;
  }

  private final Endpoints endpoints;
  private final String    queryPath;

  public HttpNodeReader(final Endpoints endpoints, final String database) {
    this.endpoints = endpoints;
    this.queryPath = "/api/v1/query/" + database;
  }

  @Override
  public long[] counts(final int node) throws IOException {
    return new long[] { count(node, ChaosSchema.COUNT_OPS), count(node, ChaosSchema.COUNT_EDGES) };
  }

  @Override
  public void scan(final int node, final NodeSnapshot sink) throws IOException {
    final String sql = ChaosSchema.page(ChaosSchema.PAGE_SIZE);
    scanPages(last -> query(node, sql, new JSONObject().put("last", last)), sink, ChaosSchema.PAGE_SIZE);
  }

  static void scanPages(final PageFetcher fetcher, final NodeSnapshot sink, final int pageSize) throws IOException {
    long last = -1;
    while (true) {
      final JSONArray rows = fetcher.fetch(last);
      for (int i = 0; i < rows.length(); i++) {
        final JSONObject row = rows.getJSONObject(i);
        last = row.getLong("id");
        sink.add(last, row.getInt("e", 0));
      }
      if (rows.length() < pageSize)
        return;
    }
  }

  private long count(final int node, final String sql) throws IOException {
    final JSONArray rows = query(node, sql, new JSONObject());
    return rows.length() == 0 ? 0 : rows.getJSONObject(0).getLong("c", 0);
  }

  private JSONArray query(final int node, final String sql, final JSONObject params) throws IOException {
    final Endpoint endpoint = endpoints.endpoint(node);
    final String payload = new JSONObject().put("language", "sql").put("command", sql).put("params", params).toString();
    final ChaosHttp.Response response = ChaosHttp.post(endpoint.host(), endpoint.port(), queryPath, payload,
        CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS);
    checkStatus(node, response.status(), response.body());
    return new JSONObject(response.body()).getJSONArray("result");
  }

  static void checkStatus(final int node, final int status, final String body) throws IOException {
    if (status >= 500)
      throw new ServerErrorException(node, status, body);
    if (status != 200)
      throw new IOException("node " + node + " query failed with HTTP " + status + ": " + body);
  }
}
