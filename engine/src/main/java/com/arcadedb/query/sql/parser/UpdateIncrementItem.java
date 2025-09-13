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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import java.util.*;

public class UpdateIncrementItem extends SimpleNode {
  protected Identifier left;
  protected Modifier   leftModifier;
  protected Expression right;

  public UpdateIncrementItem(final int id) {
    super(id);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    left.toString(params, builder);
    if (leftModifier != null) {
      leftModifier.toString(params, builder);
    }
    builder.append(" = ");
    right.toString(params, builder);
  }

  public UpdateIncrementItem copy() {
    final UpdateIncrementItem result = new UpdateIncrementItem(-1);
    result.left = left == null ? null : left.copy();
    result.leftModifier = leftModifier == null ? null : leftModifier.copy();
    result.right = right == null ? null : right.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final UpdateIncrementItem that = (UpdateIncrementItem) o;

    if (!Objects.equals(left, that.left))
      return false;
    if (!Objects.equals(leftModifier, that.leftModifier))
      return false;
    return Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    int result = left != null ? left.hashCode() : 0;
    result = 31 * result + (leftModifier != null ? leftModifier.hashCode() : 0);
    result = 31 * result + (right != null ? right.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=94dd82febb904e4e31130bdcbbb48fe3 (do not edit this line) */
