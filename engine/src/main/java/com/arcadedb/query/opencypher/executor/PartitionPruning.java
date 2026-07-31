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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.engine.Bucket;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.parser.CypherASTBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;

import java.util.List;
import java.util.Map;

/**
 * When a vertex type is partitioned by property and the pattern pins every partition property to a
 * literal, only one bucket can hold matching records, so the scan reads that bucket instead of the
 * whole type. Both Cypher executors need this, so the decision lives here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PartitionPruning {
  private PartitionPruning() {
  }

  /**
   * Returns the single bucket a pattern can be restricted to, or null when pruning does not apply.
   * <p>
   * Pruning is skipped when the type is not partitioned, when its partition mapping is stale, or when
   * any partition property is missing from the pattern or bound to something other than a literal.
   * Parameters and expressions are deliberately excluded: baking a bucket id resolved from a
   * parameter into a plan would misroute every later execution that passes a different value.
   *
   * @param type              the vertex type being scanned
   * @param patternProperties inline property map written on the node pattern
   */
  public static String prunedBucketName(final DocumentType type, final Map<String, Object> patternProperties) {
    if (type == null || !(type.getBucketSelectionStrategy() instanceof PartitionedBucketSelectionStrategy partitioned))
      return null;

    if (type instanceof LocalDocumentType localType && localType.isNeedsRepartition()) {
      // Stale mapping; nudge operators and bail.
      localType.warnIfNeedsRepartition();
      return null;
    }

    final List<String> partitionProperties = partitioned.getProperties();
    if (partitionProperties == null || partitionProperties.isEmpty())
      return null;

    if (patternProperties == null || patternProperties.isEmpty())
      return null;

    final Object[] keyValues = new Object[partitionProperties.size()];
    for (int i = 0; i < partitionProperties.size(); i++) {
      final String property = partitionProperties.get(i);
      if (!patternProperties.containsKey(property))
        return null;
      final Object value = patternProperties.get(property);
      if (value == null || value instanceof CypherASTBuilder.ParameterReference || value instanceof Expression)
        return null;
      keyValues[i] = value;
    }

    // keyValues was filled in partitionProperties order right above, so the strategy's own check is satisfied.
    final int bucketIndex = partitioned.getBucketIdByKeys(partitionProperties, keyValues, false);
    final List<? extends Bucket> typeBuckets = type.getBuckets(false);
    if (bucketIndex < 0 || bucketIndex >= typeBuckets.size())
      return null;

    return typeBuckets.get(bucketIndex).getName();
  }
}
