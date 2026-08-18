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
package com.arcadedb.query.sql.parser;

import java.util.Objects;

/**
 * Represents a single WHEN...THEN alternative in a SQL CASE expression.
 * Example: WHEN age < 18 THEN 'minor'
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CaseAlternative {
  private final WhereClause whenCondition;  // For simple CASE: WHEN condition THEN result
  private final Expression whenExpression;  // For extended CASE: WHEN value THEN result
  private final Expression thenExpression;

  /**
   * Constructor for simple CASE form (WHEN condition THEN result).
   */
  public CaseAlternative(final WhereClause whenCondition, final Expression thenExpression) {
    this.whenCondition = whenCondition;
    this.whenExpression = null;
    this.thenExpression = thenExpression;
  }

  /**
   * Constructor for extended CASE form (WHEN value THEN result).
   */
  public CaseAlternative(final Expression whenExpression, final Expression thenExpression) {
    this.whenCondition = null;
    this.whenExpression = whenExpression;
    this.thenExpression = thenExpression;
  }

  public WhereClause getWhenCondition() {
    return whenCondition;
  }

  public Expression getWhenExpression() {
    return whenExpression;
  }

  public Expression getThenExpression() {
    return thenExpression;
  }

  public boolean isSimpleForm() {
    return whenCondition != null;
  }

  /**
   * {@link CaseExpression#getIdentityElements()} carries {@code alternatives} - a {@code List<CaseAlternative>} -
   * as one element, and {@code List.equals()} compares it element by element with THIS method, so without it every
   * two alternatives fell back to reference identity (issue #6409, item 3).
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final CaseAlternative that = (CaseAlternative) o;
    return Objects.equals(whenCondition, that.whenCondition) && Objects.equals(whenExpression, that.whenExpression)
        && Objects.equals(thenExpression, that.thenExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(whenCondition, whenExpression, thenExpression);
  }
}
