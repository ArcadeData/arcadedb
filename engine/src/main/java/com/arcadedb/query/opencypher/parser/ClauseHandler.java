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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.query.opencypher.grammar.Cypher25Parser;

/**
 * Strategy interface for handling different Cypher clause types.
 * Each clause type (MATCH, CREATE, SET, etc.) has its own handler implementation.
 * This pattern replaces cascading if/else statements with a dispatch table.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@FunctionalInterface
public interface ClauseHandler {

  /**
   * Process a clause context and add the result to the statement builder.
   *
   * @param ctx the clause context to process
   * @param builder the statement builder to populate
   * @param astBuilder the AST builder for visitor method access
   */
  void handle(Cypher25Parser.ClauseContext ctx, StatementBuilder builder, CypherASTBuilder astBuilder);
}
