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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.query.opencypher.ast.Expression;

import java.util.List;

/**
 * Marker holding the right-hand-side values of a {@code WHERE x.prop IN [...]} predicate.
 * <p>
 * Used as the {@code propertyValue} of a {@link NodeIndexSeek} to drive a multi-value index seek:
 * one seek per list value, results de-duplicated by RID. Recognizing {@code IN}-lists as
 * index-seekable anchors is what lets the Cypher planner start a multi-hop pattern from the
 * filtered node and expand forward, instead of anchoring on the far side and reverse-traversing
 * (which silently returns empty over unidirectional edges or a Graph Analytical View, issue #5306).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class InListValues {
  private final List<Expression> values;

  public InListValues(final List<Expression> values) {
    this.values = values;
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public String toString() {
    return "IN " + values;
  }
}
