package com.arcadedb.query.nativ;/*
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
 */

import com.arcadedb.database.Document;

/**
 * Native condition representation in a tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeTreeNode {
  private final Object         left;
  private final NativeOperator operator;
  private       Object         right;
  private       NativeTreeNode parent;

  public NativeTreeNode(final Object left, final NativeOperator operator, final Object right) {
    this.left = left;
    this.operator = operator;
    this.right = right;
  }

  public Object eval(final Document record) {
    final Object leftValue;
    if (left instanceof NativeTreeNode)
      leftValue = ((NativeTreeNode) left).eval(record);
    else if (left instanceof NativeRuntimeValue)
      leftValue = ((NativeRuntimeValue) left).eval(record);
    else
      leftValue = left;

    final Object rightValue;
    if (right instanceof NativeTreeNode)
      rightValue = ((NativeTreeNode) right).eval(record);
    else if (right instanceof NativeRuntimeValue)
      rightValue = ((NativeRuntimeValue) right).eval(record);
    else
      rightValue = right;

    return operator.eval(leftValue, rightValue);
  }

  public void setRight(final NativeTreeNode right) {
    this.right = right;
  }

  public NativeTreeNode getParent() {
    return parent;
  }

  public void setParent(final NativeTreeNode parent) {
    this.parent = parent;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("( ");
    buffer.append(left);
    buffer.append(" ");
    buffer.append(operator);
    buffer.append(" ");
    buffer.append(right);
    buffer.append(" )");
    return buffer.toString();
  }
}
