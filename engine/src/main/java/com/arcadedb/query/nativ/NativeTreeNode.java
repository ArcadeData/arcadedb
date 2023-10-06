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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

/**
 * Native condition representation in a tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeTreeNode {
  public       Object         left;
  public final NativeOperator operator;
  public       Object         right;
  private      NativeTreeNode parent;
  public       TypeIndex      index;

  public NativeTreeNode(final Object left, final NativeOperator operator, final Object right) {
    this.left = left;
    if (left instanceof NativeTreeNode)
      ((NativeTreeNode) left).setParent(this);

    this.operator = operator;

    this.right = right;
    if (right instanceof NativeTreeNode)
      ((NativeTreeNode) right).setParent(this);
  }

  public Object eval(final Document record) {
    return operator.eval(record, left, right);
  }

  public void setRight(final NativeTreeNode right) {
    if (this.right != null)
      throw new IllegalArgumentException("Cannot assign the right node because already assigned to " + this.right);
    this.right = right;
    if (right.parent != null)
      throw new IllegalArgumentException("Cannot assign the parent to the right node " + right);
    right.parent = this;
  }

  public NativeTreeNode getParent() {
    return parent;
  }

  public void setParent(final NativeTreeNode newParent) {
    if (this.parent == newParent)
      return;

    if (this.parent != null) {
      if (this.parent.left == this) {
        this.parent.left = newParent;
      } else if (this.parent.right == this) {
        this.parent.right = newParent;
      }
    }

    this.parent = newParent;
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

  public JSONArray toJSON() {
    final JSONArray json = new JSONArray();

    if (left instanceof NativeTreeNode)
      json.put(((NativeTreeNode) left).toJSON());
    else if (left instanceof NativePropertyValue || left instanceof NativeParameterValue)
      json.put(left.toString());
    else
      json.put(left);

    if (operator != NativeOperator.run)
      json.put(operator.name);

    if (right != null) {
      if (right instanceof NativeTreeNode)
        json.put(((NativeTreeNode) right).toJSON());
      else if (right instanceof NativePropertyValue || right instanceof NativeParameterValue)
        json.put(right.toString());
      else
        json.put(right);
    }
    return json;
  }
}
