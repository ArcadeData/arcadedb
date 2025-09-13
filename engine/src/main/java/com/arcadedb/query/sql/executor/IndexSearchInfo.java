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
package com.arcadedb.query.sql.executor;

public class IndexSearchInfo {
  private final boolean        allowsRangeQueries;
  private final boolean        map;
  private final boolean        indexByKey;
  private final String         field;
  private final CommandContext context;
  private final boolean        indexByValue;
  private final boolean        supportNull;

  public IndexSearchInfo(final String indexField, final boolean allowsRangeQueries, final boolean map, final boolean indexByKey,
      final boolean indexByValue,
      final boolean supportNull, final CommandContext context) {
    this.field = indexField;
    this.allowsRangeQueries = allowsRangeQueries;
    this.map = map;
    this.indexByKey = indexByKey;
    this.context = context;
    this.indexByValue = indexByValue;
    this.supportNull = supportNull;
  }

  public String getField() {
    return field;
  }

  public CommandContext getContext() {
    return context;
  }

  public boolean allowsRange() {
    return allowsRangeQueries;
  }

  public boolean isMap() {
    return map;
  }

  public boolean isIndexByKey() {
    return indexByKey;
  }

  public boolean isIndexByValue() {
    return indexByValue;
  }

  public boolean isSupportNull() {
    return supportNull;
  }
}
