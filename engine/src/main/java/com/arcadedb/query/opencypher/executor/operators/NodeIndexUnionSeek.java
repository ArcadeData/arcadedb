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
package com.arcadedb.query.opencypher.executor.operators;

import java.util.List;

/**
 * Physical operator for an OR of index-served predicates on one node, such as
 * {@code MATCH (n:T) WHERE n.x = $a OR n.s = $b} (issue #8723): one {@link NodeIndexSeek} per disjunct, each on the index
 * of its own property, run in sequence with the vertices de-duplicated by RID, since a vertex that satisfies two
 * disjuncts is found by both seeks. The union of the seeks covers every vertex the OR can keep, and the WHERE is still
 * evaluated above the anchor, so a seek that finds more than the disjunct keeps is harmless.
 * <p>
 * The union and the de-duplication are those of {@link NodeByLabelDisjunctionIndexSeek}, which unions the seeks of the
 * root types of a label disjunction; here every seek is on the same type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NodeIndexUnionSeek extends NodeByLabelDisjunctionIndexSeek {
  public NodeIndexUnionSeek(final String variable, final List<NodeIndexSeek> seeks, final double estimatedCost,
      final long estimatedCardinality) {
    super(variable, seeks, estimatedCost, estimatedCardinality);
  }

  @Override
  public String getOperatorType() {
    return "NodeIndexUnionSeek";
  }

  @Override
  protected String seekUnit() {
    return "seeks";
  }
}
