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
package com.arcadedb.query.opencypher.ast;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a MERGE clause in a Cypher query.
 * Ensures a pattern exists in the graph: matches if it exists, creates if it doesn't.
 * <p>
 * Examples:
 * - MERGE (n:Person {name: 'Alice'})
 * - MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
 * - MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = timestamp() ON MATCH SET n.updated = timestamp()
 * <p>
 * Supports ON CREATE SET and ON MATCH SET sub-clauses. Each may be repeated: every repetition is kept as its own
 * {@link SetClause} and applied in source order, so a later one reads the writes of an earlier one (issue #11691).
 * {@link #getOnCreateSet()} / {@link #getOnMatchSet()} expose all the items of one branch as a single clause, for the
 * analyses that only need to know what the branch reads and writes.
 */
public class MergeClause {
  private final PathPattern pathPattern;
  private final SetClause       onCreateSet;
  private final SetClause       onMatchSet;
  private final List<SetClause> onCreateActions;
  private final List<SetClause> onMatchActions;

  public MergeClause(final PathPattern pathPattern) {
    this(pathPattern, List.of(), List.of());
  }

  public MergeClause(final PathPattern pathPattern, final List<SetClause> onCreateActions, final List<SetClause> onMatchActions) {
    this.pathPattern = pathPattern;
    this.onCreateActions = onCreateActions;
    this.onMatchActions = onMatchActions;
    this.onCreateSet = combine(onCreateActions);
    this.onMatchSet = combine(onMatchActions);
  }

  private static SetClause combine(final List<SetClause> actions) {
    if (actions.isEmpty())
      return null;
    if (actions.size() == 1)
      return actions.getFirst();
    final List<SetClause.SetItem> items = new ArrayList<>();
    for (final SetClause action : actions)
      items.addAll(action.getItems());
    return new SetClause(items);
  }

  /** The ON CREATE SET clauses in source order, each to be applied as a SET of its own. */
  public List<SetClause> getOnCreateActions() {
    return onCreateActions;
  }

  /** The ON MATCH SET clauses in source order, each to be applied as a SET of its own. */
  public List<SetClause> getOnMatchActions() {
    return onMatchActions;
  }

  public PathPattern getPathPattern() {
    return pathPattern;
  }

  public SetClause getOnCreateSet() {
    return onCreateSet;
  }

  public SetClause getOnMatchSet() {
    return onMatchSet;
  }

  public boolean hasOnCreateSet() {
    return onCreateSet != null && !onCreateSet.isEmpty();
  }

  public boolean hasOnMatchSet() {
    return onMatchSet != null && !onMatchSet.isEmpty();
  }
}
