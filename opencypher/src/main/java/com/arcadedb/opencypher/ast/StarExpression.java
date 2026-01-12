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
package com.arcadedb.opencypher.ast;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * Expression representing the asterisk (*) used in count(*).
 * Always evaluates to a non-null marker to indicate "count this row".
 */
public class StarExpression implements Expression {
  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    // Return a non-null marker value so count(*) counts the row
    return "*";
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public String getText() {
    return "*";
  }
}
