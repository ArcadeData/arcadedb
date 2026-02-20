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
package com.arcadedb.remote;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;

public class RemoteMaterializedView implements MaterializedView {
  private final String name;
  private final String query;
  private final String backingTypeName;
  private final MaterializedViewRefreshMode refreshMode;
  private final boolean simpleQuery;
  private final long refreshInterval;
  private final long lastRefreshTime;
  private final String status;
  private final List<String> sourceTypeNames;
  private final long refreshCount;
  private final long refreshTotalTimeMs;
  private final long refreshMinTimeMs;
  private final long refreshMaxTimeMs;
  private final long errorCount;
  private final long lastRefreshDurationMs;

  public RemoteMaterializedView(final Result result) {
    this.name = result.getProperty("name");
    this.query = result.getProperty("query");
    this.backingTypeName = result.getProperty("backingType");
    this.refreshMode = MaterializedViewRefreshMode.valueOf(result.getProperty("refreshMode"));
    this.simpleQuery = result.getProperty("simpleQuery") != null ? (Boolean) result.getProperty("simpleQuery") : false;
    this.refreshInterval = result.getProperty("refreshInterval") != null ? ((Number) result.getProperty("refreshInterval")).longValue() : 0;
    this.lastRefreshTime = result.getProperty("lastRefreshTime") != null ? ((Number) result.getProperty("lastRefreshTime")).longValue() : 0;
    this.status = result.getProperty("status") != null ? result.getProperty("status") : "VALID";
    final List<String> srcTypes = result.getProperty("sourceTypes");
    this.sourceTypeNames = srcTypes != null ? srcTypes : List.of();
    this.refreshCount = result.getProperty("refreshCount") != null ? ((Number) result.getProperty("refreshCount")).longValue() : 0;
    this.refreshTotalTimeMs = result.getProperty("refreshTotalTimeMs") != null ? ((Number) result.getProperty("refreshTotalTimeMs")).longValue() : 0;
    this.refreshMinTimeMs = result.getProperty("refreshMinTimeMs") != null ? ((Number) result.getProperty("refreshMinTimeMs")).longValue() : 0;
    this.refreshMaxTimeMs = result.getProperty("refreshMaxTimeMs") != null ? ((Number) result.getProperty("refreshMaxTimeMs")).longValue() : 0;
    this.errorCount = result.getProperty("errorCount") != null ? ((Number) result.getProperty("errorCount")).longValue() : 0;
    this.lastRefreshDurationMs = result.getProperty("lastRefreshDurationMs") != null ? ((Number) result.getProperty("lastRefreshDurationMs")).longValue() : 0;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public DocumentType getBackingType() {
    throw new UnsupportedOperationException("getBackingType() is not supported in remote materialized view");
  }

  @Override
  public List<String> getSourceTypeNames() {
    return sourceTypeNames;
  }

  @Override
  public MaterializedViewRefreshMode getRefreshMode() {
    return refreshMode;
  }

  @Override
  public long getLastRefreshTime() {
    return lastRefreshTime;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public boolean isSimpleQuery() {
    return simpleQuery;
  }

  @Override
  public long getRefreshInterval() {
    return refreshInterval;
  }

  @Override
  public long getRefreshCount() {
    return refreshCount;
  }

  @Override
  public long getRefreshTotalTimeMs() {
    return refreshTotalTimeMs;
  }

  @Override
  public long getRefreshMinTimeMs() {
    return refreshMinTimeMs;
  }

  @Override
  public long getRefreshMaxTimeMs() {
    return refreshMaxTimeMs;
  }

  @Override
  public long getErrorCount() {
    return errorCount;
  }

  @Override
  public long getLastRefreshDurationMs() {
    return lastRefreshDurationMs;
  }

  @Override
  public void refresh() {
    throw new UnsupportedOperationException("refresh() is not supported in remote materialized view. Use SQL REFRESH MATERIALIZED VIEW instead.");
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("drop() is not supported in remote materialized view. Use SQL DROP MATERIALIZED VIEW instead.");
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("query", query);
    json.put("backingType", backingTypeName);
    json.put("refreshMode", refreshMode.name());
    json.put("simpleQuery", simpleQuery);
    json.put("refreshInterval", refreshInterval);
    json.put("lastRefreshTime", lastRefreshTime);
    json.put("status", status);
    if (sourceTypeNames != null) {
      final JSONArray srcTypes = new JSONArray();
      for (final String src : sourceTypeNames)
        srcTypes.put(src);
      json.put("sourceTypes", srcTypes);
    }
    return json;
  }
}
