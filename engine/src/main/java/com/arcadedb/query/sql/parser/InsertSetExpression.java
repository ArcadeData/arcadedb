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

import java.util.Map;
import java.util.Objects;

/**
 * Created by luigidellaquila on 19/02/15.
 */
public class InsertSetExpression {

  public Identifier left;
  public Expression right;

  public InsertSetExpression() {
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" = ");
    right.toString(params, builder);
  }

  public InsertSetExpression copy() {
    final InsertSetExpression result = new InsertSetExpression();
    result.left = left == null ? null : left.copy();
    result.right = right == null ? null : right.copy();
    return result;
  }

  public Identifier getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  /**
   * {@link InsertBody#equals(Object)} carries {@code setExpressions} - a {@code List<InsertSetExpression>} - through
   * {@code Objects.equals()}, and {@code List.equals()} compares it element by element with THIS method, so without
   * it two parses of the same {@code INSERT ... SET} statement fell back to reference identity and never compared
   * equal (issue #6409, item 3).
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final InsertSetExpression that = (InsertSetExpression) o;
    return Objects.equals(left, that.left) && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right);
  }
}
