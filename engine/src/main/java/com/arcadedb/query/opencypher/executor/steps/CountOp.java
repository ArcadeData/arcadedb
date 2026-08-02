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
   * Every operator here anchors its walk on one labelled position of the pattern and enumerates that label's
   * buckets - {@code MATCH (a:Person)-[:KNOWS]->(b)} starts from the {@code Person} vertices. An <b>unlabelled</b>
   * anchor has no bucket set to enumerate, and each operator reads that missing set as an empty one and answers
   * <b>0</b>, which for {@code MATCH (a)-[:KNOWS]->(b) RETURN count(*)} is a wrong answer rather than a slow one
   * (issue #5715).
   * <p>
   * An operator that cannot enumerate its anchors is not built at all, and the ordinary materialization pipeline -
   * which starts from every vertex - answers the query instead.
   */
  default boolean canEnumerateAnchors() {
    return true;
  }

  /**
   * Returns the prettyPrint description for execution plan display.
   */
  String describe(int depth, int indent);
}
