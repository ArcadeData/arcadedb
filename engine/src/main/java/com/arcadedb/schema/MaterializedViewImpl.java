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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MaterializedViewImpl implements MaterializedView {
  private final Database database;
  private final String name;
  private final String query;
  private final String backingTypeName;
  private final List<String> sourceTypeNames;
  private final MaterializedViewRefreshMode refreshMode;
  private final boolean simpleQuery;
  private final long refreshInterval;
  private volatile long lastRefreshTime;
  private volatile String status;
  private transient MaterializedViewChangeListener changeListener;

  public MaterializedViewImpl(final Database database, final String name, final String query,
      final String backingTypeName, final List<String> sourceTypeNames,
      final MaterializedViewRefreshMode refreshMode, final boolean simpleQuery,
      final long refreshInterval) {
    this.database = database;
    this.name = name;
    this.query = query;
    this.backingTypeName = backingTypeName;
    this.sourceTypeNames = List.copyOf(sourceTypeNames);
    this.refreshMode = refreshMode;
    this.simpleQuery = simpleQuery;
    this.refreshInterval = refreshInterval;
    this.lastRefreshTime = 0;
    this.status = "VALID";
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
    return database.getSchema().getType(backingTypeName);
  }

  public String getBackingTypeName() {
    return backingTypeName;
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

  public long getRefreshInterval() {
    return refreshInterval;
  }

  public void setStatus(final String status) {
    this.status = status;
  }

  public MaterializedViewChangeListener getChangeListener() {
    return changeListener;
  }

  public void setChangeListener(final MaterializedViewChangeListener listener) {
    this.changeListener = listener;
  }

  public void updateLastRefreshTime() {
    this.lastRefreshTime = System.currentTimeMillis();
  }

  public void setLastRefreshTime(final long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  MaterializedViewImpl copyWithRefreshMode(final MaterializedViewRefreshMode newMode,
      final long newInterval) {
    final MaterializedViewImpl copy = new MaterializedViewImpl(
        database, name, query, backingTypeName, sourceTypeNames,
        newMode, simpleQuery, newInterval);
    copy.lastRefreshTime = this.lastRefreshTime;
    copy.status = this.status;
    return copy;
  }

  @Override
  public void refresh() {
    MaterializedViewRefresher.fullRefresh(database, this);
  }

  @Override
  public void drop() {
    database.getSchema().dropMaterializedView(name);
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
    final JSONArray srcTypes = new JSONArray();
    for (final String src : sourceTypeNames)
      srcTypes.put(src);
    json.put("sourceTypes", srcTypes);
    return json;
  }

  public static MaterializedViewImpl fromJSON(final Database database, final JSONObject json) {
    final List<String> sourceTypes = new ArrayList<>();
    final JSONArray srcArray = json.getJSONArray("sourceTypes");
    for (int i = 0; i < srcArray.length(); i++)
      sourceTypes.add(srcArray.getString(i));

    final MaterializedViewImpl view = new MaterializedViewImpl(
        database,
        json.getString("name"),
        json.getString("query"),
        json.getString("backingType"),
        sourceTypes,
        MaterializedViewRefreshMode.valueOf(json.getString("refreshMode")),
        json.getBoolean("simpleQuery", false),
        json.getLong("refreshInterval", 0));
    view.lastRefreshTime = json.getLong("lastRefreshTime", 0);
    view.status = json.getString("status", "VALID");
    return view;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final MaterializedViewImpl that = (MaterializedViewImpl) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return "MaterializedView{name='" + name + "', refreshMode=" + refreshMode +
        ", status=" + status + ", simpleQuery=" + simpleQuery + '}';
  }
}
