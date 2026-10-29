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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.test.support.FacetSchema;
import com.arcadedb.test.support.FacetSnapshot;

import java.io.IOException;
import java.util.SortedMap;

/**
 * {@link FacetReader} over each node's own {@code /api/v1/query} endpoint.
 */
public final class HttpFacetReader implements FacetReader {
  private final Endpoints endpoints;
  private final String    queryPath;

  public HttpFacetReader(final Endpoints endpoints, final String database) {
    this.endpoints = endpoints;
    this.queryPath = "/api/v1/query/" + database;
  }

  @Override
  public SortedMap<String, Long> groupBy(final int node, final String where) throws IOException {
    return FacetSchema.expectedCounts(sql -> post(node, sql).getJSONArray("result"), where);
  }

  @Override
  public FacetSnapshot facets(final int node, final String where) throws IOException {
    return FacetSnapshot.parse(post(node, FacetSchema.facetQuery(where)));
  }

  private JSONObject post(final int node, final String sql) throws IOException {
    return HttpNodeReader.post(endpoints, node, queryPath, sql, new JSONObject());
  }
}
