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
 * Represents a single WHEN...THEN alternative in a CASE expression.
 * Example: WHEN age < 18 THEN 'minor'
 */
public class CaseAlternative {
  private final Expression whenExpression;
  private final Expression thenExpression;

  public CaseAlternative(final Expression whenExpression, final Expression thenExpression) {
    this.whenExpression = whenExpression;
    this.thenExpression = thenExpression;
  }

  public Expression getWhenExpression() {
    return whenExpression;
  }

  public Expression getThenExpression() {
    return thenExpression;
  }
}
