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

import java.util.Set;

/**
 * A step that enforces Cypher's relationship uniqueness, which is scoped to a single MATCH clause: within
 * one clause every relationship pattern must match a distinct edge, and across clauses nothing is implied.
 * <p>
 * The rule therefore ranges over the variables that MATCH clause itself binds, and over those only. An edge
 * the incoming row already carries under a name some earlier clause bound - a {@code WITH}, an
 * {@code UNWIND}, a {@code CALL ... YIELD}, a {@code CALL { }} subquery, a {@code CREATE} - is not a
 * relationship of this clause's pattern, so it must not stop the pattern from matching that same edge again.
 * <p>
 * Naming the clause's own variables is what makes that decidable. Asking the opposite question - which names
 * came from somewhere else - needs every clause that binds anything to remember to declare it, and the clause
 * that forgets fails by silently dropping every row rather than by raising anything (issue #7165).
 * <p>
 * The set is complete only once the whole MATCH clause has been planned, since a later hop of the same clause
 * still adds to it, so the planner hands it over after building the clause instead of through the constructor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface ClauseScopedUniquenessStep {
  /**
   * Declares the variables bound by the MATCH clause this step belongs to. Never null; an empty set means the
   * clause binds nothing this step could collide with.
   */
  void setClauseScopeVariables(Set<String> clauseScopeVariables);
}
