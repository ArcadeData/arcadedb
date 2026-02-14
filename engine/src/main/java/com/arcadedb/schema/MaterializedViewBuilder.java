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

import com.arcadedb.database.DatabaseInternal;

public class MaterializedViewBuilder {
  private final DatabaseInternal database;
  private String name;
  private String query;
  private MaterializedViewRefreshMode refreshMode = MaterializedViewRefreshMode.MANUAL;
  private int buckets = 0;
  private int pageSize = 0;
  private long refreshInterval = 0;
  private boolean ifNotExists = false;

  public MaterializedViewBuilder(final DatabaseInternal database) {
    this.database = database;
  }

  public MaterializedViewBuilder withName(final String name) {
    this.name = name;
    return this;
  }

  public MaterializedViewBuilder withQuery(final String query) {
    this.query = query;
    return this;
  }

  public MaterializedViewBuilder withRefreshMode(final MaterializedViewRefreshMode mode) {
    this.refreshMode = mode;
    return this;
  }

  public MaterializedViewBuilder withTotalBuckets(final int buckets) {
    this.buckets = buckets;
    return this;
  }

  public MaterializedViewBuilder withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public MaterializedViewBuilder withRefreshInterval(final long intervalMs) {
    this.refreshInterval = intervalMs;
    return this;
  }

  public MaterializedViewBuilder withIgnoreIfExists(final boolean ignore) {
    this.ifNotExists = ignore;
    return this;
  }

  public MaterializedView create() {
    // TODO: full implementation in Task 8
    throw new UnsupportedOperationException("MaterializedViewBuilder.create() not yet implemented");
  }
}
