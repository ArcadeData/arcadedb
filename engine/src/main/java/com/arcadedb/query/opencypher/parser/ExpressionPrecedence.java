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

/**
 * Documents the expression precedence hierarchy in Cypher.
 * This enum serves as documentation for the parsing order in CypherExpressionBuilder.
 *
 * Precedence levels (from lowest to highest):
 * 1. LOGICAL_OR - OR operator (lowest precedence, parsed first)
 * 2. LOGICAL_XOR - XOR operator
 * 3. LOGICAL_AND - AND operator
 * 4. LOGICAL_NOT - NOT operator (unary)
 * 5. COMPARISON - =, <>, <, >, <=, >=
 * 6. STRING_COMPARISON - STARTS WITH, ENDS WITH, CONTAINS, IN, =~, IS NULL
 * 7. ARITHMETIC_ADD - + (addition, string/list concatenation)
 * 8. ARITHMETIC_MULTIPLY - *, /, %
 * 9. ARITHMETIC_POWER - ^ (exponentiation)
 * 10. UNARY - Unary -, + operators
 * 11. POSTFIX - Property access, indexing, slicing
 * 12. PRIMARY - Literals, variables, function calls, parenthesized expressions (highest precedence, evaluated first)
 *
 * Special constructs (parsed before precedence-based parsing):
 * - SPECIAL_FUNCTIONS: count(*), EXISTS, CASE
 * - COMPREHENSIONS: List comprehensions, pattern comprehensions, reduce
 * - PREDICATES: all(), any(), none(), single()
 * - PATHS: shortestPath(), allShortestPaths()
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
enum ExpressionPrecedence {
  // Special constructs (highest priority in parsing)
  SPECIAL_FUNCTIONS,    // count(*), EXISTS, CASE, shortestPath
  COMPREHENSIONS,       // List/pattern comprehensions, reduce
  PREDICATES,           // all(), any(), none(), single()

  // Standard precedence levels (from lowest to highest)
  LOGICAL_OR,           // OR
  LOGICAL_XOR,          // XOR
  LOGICAL_AND,          // AND
  LOGICAL_NOT,          // NOT
  COMPARISON,           // =, <>, <, >, <=, >=
  STRING_COMPARISON,    // STARTS WITH, ENDS WITH, CONTAINS, IN, =~, IS NULL
  ARITHMETIC_ADD,       // +, - (binary)
  ARITHMETIC_MULTIPLY,  // *, /, %
  ARITHMETIC_POWER,     // ^
  UNARY,                // Unary -, +, NOT
  POSTFIX,              // Property access, indexing, slicing
  PRIMARY;              // Literals, variables, function calls, parenthesized (highest)

  /**
   * Get a human-readable description of this precedence level.
   */
  public String getDescription() {
    return switch (this) {
      case SPECIAL_FUNCTIONS -> "Special functions: count(*), EXISTS, CASE, shortestPath";
      case COMPREHENSIONS -> "List/pattern comprehensions, reduce expressions";
      case PREDICATES -> "List predicates: all(), any(), none(), single()";
      case LOGICAL_OR -> "Logical OR (lowest operator precedence)";
      case LOGICAL_XOR -> "Logical XOR";
      case LOGICAL_AND -> "Logical AND";
      case LOGICAL_NOT -> "Logical NOT (unary)";
      case COMPARISON -> "Comparison: =, <>, <, >, <=, >=";
      case STRING_COMPARISON -> "String/List comparison: STARTS WITH, ENDS WITH, CONTAINS, IN, =~, IS NULL";
      case ARITHMETIC_ADD -> "Additive: +, - (also string/list concatenation)";
      case ARITHMETIC_MULTIPLY -> "Multiplicative: *, /, %";
      case ARITHMETIC_POWER -> "Exponentiation: ^";
      case UNARY -> "Unary operators: -, +";
      case POSTFIX -> "Postfix: property access (.prop), indexing ([n]), slicing ([n..m])";
      case PRIMARY -> "Primary: literals, variables, function calls, parenthesized expressions (highest precedence)";
    };
  }
}
