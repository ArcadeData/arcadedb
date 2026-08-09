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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.graph.GraphTraversalProvider;

/**
 * Interface for count-push-down operators used by {@link CSRCountStep}.
 * Each implementation handles a specific pattern (chain, star-join, triangle, pair-join)
 * and delegates common boilerplate (provider lookup, profiling, result packaging) to CSRCountStep.
 */
public interface CountOp {
  /**
   * Executes the count using the CSR/GAV provider.
   */
  long execute(GraphTraversalProvider provider, Database db);

  /**
   * OLTP fallback when no CSR provider is available.
   */
  long executeOLTP(Database db);

  /**
   * Returns all edge types needed by this operator (for provider lookup).
   */
  String[] edgeTypes();

  /**
   * Whether this operator can enumerate the set of vertices it starts from.
   * <p>
   * An operator anchors its walk on one position of the pattern and enumerates the vertices that position accepts -
   * {@code MATCH (a:Person)-[:KNOWS]->(b)} starts from the {@code Person} vertices. When that position carries no
   * label the anchors are <b>every vertex</b>, and an operator that read the missing bucket set as an empty one
   * answered <b>0</b> for a pattern that matches (issue #5715).
   * <p>
   * The two chain operators now enumerate the unlabelled case for what it is (issue #5757) and always answer true
   * here. The ones that still cannot - they key a hash join or a degree product on the label itself - decline, are
   * not built at all, and the ordinary materialization pipeline answers the query instead.
   */
  default boolean canEnumerateAnchors() {
    return true;
  }

  /**
   * Whether this operator's anchors are <b>every vertex</b> rather than one label's, which makes it answerable off a
   * {@link GraphTraversalProvider} only when that provider's node domain is every vertex too.
   * <p>
   * A view built over a subset of the vertex types is a perfectly good accelerator for a walk anchored on a label it
   * holds, but "every vertex" is a claim about the graph rather than about the view, and a view holding some of them
   * cannot make it. Such an operator runs against the OLTP path instead, which reads the schema (issue #5757).
   */
  default boolean requiresFullVertexCoverage() {
    return false;
  }

  /**
   * Returns the prettyPrint description for execution plan display.
   */
  String describe(int depth, int indent);
}
