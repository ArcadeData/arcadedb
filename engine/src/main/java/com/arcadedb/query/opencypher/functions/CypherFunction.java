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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.function.StatelessFunction;

/**
 * Interface for namespaced Cypher functions (e.g., text.indexOf, map.merge).
 * <p>
 * Functions are stateless operations that transform input values to output values.
 * For operations that modify the database or return multiple rows, use {@link com.arcadedb.query.opencypher.procedures.CypherProcedure} instead.
 * </p>
 * <p>
 * This interface extends {@link StatelessFunction} making all Cypher functions available
 * in the unified {@link com.arcadedb.function.FunctionRegistry}.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see StatelessFunction
 * @see com.arcadedb.function.FunctionRegistry
 */
public interface CypherFunction extends StatelessFunction {
  // All methods inherited from StatelessFunction and Function
  // Implementations remain compatible - no changes needed
}
