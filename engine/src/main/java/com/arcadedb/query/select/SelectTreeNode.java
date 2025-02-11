package com.arcadedb.query.select;/*
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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONArray;

/**
 * Native condition representation in a tree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectTreeNode {
  public       Object         left;
  public final SelectOperator operator;
  public       Object         right;
  private      SelectTreeNode parent;
  public       TypeIndex      index;

  public SelectTreeNode(final Object left, final SelectOperator operator, final Object right) {
    this.left = left;
    if (left instanceof SelectTreeNode node)
      node.setParent(this);

    this.operator = operator;

    this.right = right;
    if (right instanceof SelectTreeNode node)
      node.setParent(this);
  }

  public Object eval(final Document record) {
    return operator.eval(record, left, right);
  }

  public void setRight(final SelectTreeNode right) {
    if (this.right != null)
      throw new IllegalArgumentException("Cannot assign the right node because already assigned to " + this.right);
    this.right = right;
    if (right.parent != null)
      throw new IllegalArgumentException("Cannot assign the parent to the right node " + right);
    right.parent = this;
  }

  public SelectTreeNode getParent() {
    return parent;
  }

  public void setParent(final SelectTreeNode newParent) {
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

    if (left instanceof SelectTreeNode node)
      json.put(node.toJSON());
    else if (left instanceof SelectPropertyValue || left instanceof SelectParameterValue)
      json.put(left.toString());
    else
      json.put(left);

    if (operator != SelectOperator.run)
      json.put(operator.name);

    if (right != null) {
      if (right instanceof SelectTreeNode node)
        json.put(node.toJSON());
      else if (right instanceof SelectPropertyValue || right instanceof SelectParameterValue)
        json.put(right.toString());
      else
        json.put(right);
    }
    return json;
  }
}
