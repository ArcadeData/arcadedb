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

import java.math.BigDecimal;
import java.util.Map;

/**
 * One half-open range {@code [from, to) [AS 'label']} of a {@code RANGES} facet. A bound is {@code null} for {@code *}
 * (unbounded), a {@link Number} or a {@link String} as written, or the {@link InputParameter} written in its place, resolved
 * when the facet is compiled.
 */
public class FacetRangeItem extends SimpleNode {
  public Object from;
  public Object to;
  public String label;

  public FacetRangeItem() {
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append('[');
    appendBound(params, builder, from);
    builder.append(", ");
    appendBound(params, builder, to);
    builder.append(')');
    if (label != null) {
      builder.append(" AS ");
      appendQuotedStringLiteral(builder, label);
    }
  }

  /**
   * Writes a bound so that it parses back to the same value and Java type: {@code *} for null, a number with the suffix its
   * type needs, a quoted string, or the parameter.
   */
  static void appendBound(final Map<String, Object> params, final StringBuilder builder, final Object bound) {
    switch (bound) {
    case null -> builder.append('*');
    case InputParameter parameter -> parameter.toString(params, builder);
    case String string -> appendQuotedStringLiteral(builder, string);
    case Long l -> builder.append(l).append('L');
    case Float f -> builder.append(f).append('f');
    case BigDecimal d -> builder.append(d.toPlainString());
    default -> builder.append(bound);
    }
  }

  static Object copyBound(final Object bound) {
    return bound instanceof InputParameter parameter ? parameter.copy() : bound;
  }

  static Object boundToJSON(final Object bound) {
    return bound instanceof InputParameter parameter ? parameter.toString() : bound;
  }

  @Override
  public FacetRangeItem copy() {
    final FacetRangeItem result = new FacetRangeItem();
    result.from = copyBound(from);
    result.to = copyBound(to);
    result.label = label;
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { from, to, label };
  }

  @Override
  public Map<String, Object> toJSON() {
    final Map<String, Object> json = super.toJSON();
    json.put("from", boundToJSON(from));
    json.put("to", boundToJSON(to));
    if (label != null)
      json.put("label", label);
    return json;
  }
}
