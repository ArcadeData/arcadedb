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

/**
 * Represents an UNWIND clause in a Cypher query.
 * Expands a list into individual rows, one for each element.
 * <p>
 * Examples:
 * - UNWIND [1, 2, 3] AS x RETURN x
 * - UNWIND range(1, 10) AS num RETURN num
 * - MATCH (n:Person) UNWIND n.hobbies AS hobby RETURN n.name, hobby
 */
public class UnwindClause {
  private final Expression listExpression;
  private final String variable;

  public UnwindClause(final Expression listExpression, final String variable) {
    this.listExpression = listExpression;
    this.variable = variable;
  }

  public Expression getListExpression() {
    return listExpression;
  }

  public String getVariable() {
    return variable;
  }
}
