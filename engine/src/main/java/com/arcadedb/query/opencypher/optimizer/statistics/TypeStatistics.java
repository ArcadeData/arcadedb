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

/**
 * Statistics for a single type (vertex or edge type).
 * Stores cardinality and metadata needed for cost estimation.
 */
public class TypeStatistics {
  private final String typeName;
  private final long recordCount;
  private final boolean isVertexType;

  public TypeStatistics(final String typeName, final long recordCount, final boolean isVertexType) {
    this.typeName = typeName;
    this.recordCount = recordCount;
    this.isVertexType = isVertexType;
  }

  /**
   * Returns the name of the type.
   */
  public String getTypeName() {
    return typeName;
  }

  /**
   * Returns the total number of records of this type.
   * Uses cached O(1) count from Bucket.count().
   */
  public long getRecordCount() {
    return recordCount;
  }

  /**
   * Returns true if this is a vertex type, false if edge type.
   */
  public boolean isVertexType() {
    return isVertexType;
  }

  @Override
  public String toString() {
    return "TypeStatistics{" +
        "typeName='" + typeName + '\'' +
        ", recordCount=" + recordCount +
        ", isVertexType=" + isVertexType +
        '}';
  }
}
