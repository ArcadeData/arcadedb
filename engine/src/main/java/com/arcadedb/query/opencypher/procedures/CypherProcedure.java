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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.function.procedure.Procedure;

/**
 * Interface for namespaced Cypher procedures (e.g., merge.relationship, algo.dijkstra).
 * <p>
 * Procedures differ from functions in that they:
 * <ul>
 *   <li>Can return multiple rows (via Stream)</li>
 *   <li>Support the YIELD clause for selecting output fields</li>
 *   <li>Can modify the database (create nodes, relationships, etc.)</li>
 *   <li>Can access the input row context for per-row execution</li>
 * </ul>
 * </p>
 * <p>
 * This interface extends {@link Procedure} making all Cypher procedures available
 * in the unified {@link com.arcadedb.function.procedure.ProcedureRegistry}.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL merge.relationship(a, 'KNOWS', {}, {since: 2020}, b) YIELD rel
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see Procedure
 * @see com.arcadedb.function.procedure.ProcedureRegistry
 */
public interface CypherProcedure extends Procedure {
  // All methods inherited from Procedure
  // Implementations remain compatible - no changes needed
}
