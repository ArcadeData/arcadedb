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
package com.arcadedb.query.opencypher.optimizer.statistics;

import java.util.Arrays;
import java.util.List;

/**
 * Statistics for a single index.
 * Stores metadata needed to decide whether to use the index for a query.
 */
public class IndexStatistics {
  private final String typeName;
  private final List<String> propertyNames;
  private final boolean isUnique;
  private final String indexName;

  public IndexStatistics(final String typeName, final List<String> propertyNames,
                        final boolean isUnique, final String indexName) {
    this.typeName = typeName;
    this.propertyNames = propertyNames;
    this.isUnique = isUnique;
    this.indexName = indexName;
  }

  /**
   * Returns the type name this index is defined on.
   */
  public String getTypeName() {
    return typeName;
  }

  /**
   * Returns the list of property names covered by this index.
   * For composite indexes, properties are in index definition order.
   */
  public List<String> getPropertyNames() {
    return propertyNames;
  }

  /**
   * Returns true if this is a unique index.
   * Unique indexes have selectivity = 1/typeCount (very selective).
   */
  public boolean isUnique() {
    return isUnique;
  }

  /**
   * Returns the internal index name.
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * Checks if this index can be used for a given property.
   * For composite indexes, only the first property can be used for seeks.
   */
  public boolean canBeUsedForProperty(final String propertyName) {
    return !propertyNames.isEmpty() && propertyNames.get(0).equals(propertyName);
  }

  /**
   * Checks if this index covers all the given properties in order.
   */
  public boolean coversProperties(final String... properties) {
    if (properties.length > propertyNames.size()) {
      return false;
    }
    for (int i = 0; i < properties.length; i++) {
      if (!propertyNames.get(i).equals(properties[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "IndexStatistics{" +
        "typeName='" + typeName + '\'' +
        ", propertyNames=" + propertyNames +
        ", isUnique=" + isUnique +
        ", indexName='" + indexName + '\'' +
        '}';
  }
}
