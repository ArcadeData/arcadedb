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
package com.arcadedb.engine.timeseries.promql.ast;

import java.util.List;

/**
 * Sealed interface representing all PromQL AST node types.
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public sealed interface PromQLExpr {

  record NumberLiteral(double value) implements PromQLExpr {
  }

  record StringLiteral(String value) implements PromQLExpr {
  }

  record VectorSelector(String metricName, List<LabelMatcher> matchers, long offsetMs) implements PromQLExpr {
  }

  record MatrixSelector(VectorSelector selector, long rangeMs) implements PromQLExpr {
  }

  record AggregationExpr(AggOp op, PromQLExpr expr, List<String> groupLabels, boolean without,
                          PromQLExpr param) implements PromQLExpr {
  }

  record FunctionCallExpr(String name, List<PromQLExpr> args) implements PromQLExpr {
  }

  record BinaryExpr(PromQLExpr left, BinaryOp op, PromQLExpr right) implements PromQLExpr {
  }

  record UnaryExpr(char op, PromQLExpr expr) implements PromQLExpr {
  }

  enum AggOp {
    SUM, AVG, MIN, MAX, COUNT, TOPK, BOTTOMK
  }

  enum BinaryOp {
    ADD("+"), SUB("-"), MUL("*"), DIV("/"), MOD("%"), POW("^"),
    EQ("=="), NEQ("!="), LT("<"), GT(">"), LTE("<="), GTE(">="),
    AND("and"), OR("or"), UNLESS("unless");

    private final String symbol;

    BinaryOp(final String symbol) {
      this.symbol = symbol;
    }

    public String symbol() {
      return symbol;
    }
  }

  enum MatchOp {
    EQ, NEQ, RE, NRE
  }

  record LabelMatcher(String name, MatchOp op, String value) {
  }
}
