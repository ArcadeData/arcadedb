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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One facet of a {@code FACETS} clause: {@code [alias:] expression KIND options}. The parser fills only the fields its kind
 * accepts; an option not written is {@code null} (or {@code false} for {@link #missing}), and its default is applied when the
 * facet is compiled. A bound ({@link #boundsFrom}, {@link #boundsTo}) follows the rules of {@link FacetRangeItem}.
 */
public class FacetItem extends SimpleNode {
  public enum Kind {TERMS, RANGES, HISTOGRAM, HIERARCHY, DISTINCT, STATS}

  /** The alias as written, null when the facet is named after its expression (see {@link #getName()}). */
  public Identifier           alias;
  public Expression           expression;
  public Kind                 kind;
  public PInteger             top;
  public PInteger             minCount;
  public WhereClause          filter;
  public List<Expression>     include;
  /** TRUE for {@code ORDER BY COUNT}, FALSE for {@code ORDER BY VALUE}, null when not written. */
  public Boolean              orderByCount;
  /** TRUE for {@code ASC}, FALSE for {@code DESC}, null when not written. */
  public Boolean              ascending;
  public boolean              missing;
  public List<FacetRangeItem> ranges;
  public String               defaultLabel;
  /** TRUE for {@code CALENDAR}, FALSE for {@code FIXED}, null for any kind but HISTOGRAM. */
  public Boolean              calendar;
  /** The histogram interval as written: a {@link String} or a {@link Number}. */
  public Object               interval;
  public String               timezone;
  public String               offset;
  public Object               boundsFrom;
  public Object               boundsTo;

  public FacetItem() {
  }

  /**
   * The name of the facet in the results: the alias, or, when no alias is written, the name a projection of the expression gets
   * (a property without its back-ticks, else the text of the expression).
   */
  public String getName() {
    return alias != null ? alias.getStringValue() : expression.getDefaultAlias().getStringValue();
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    if (alias != null) {
      alias.toString(params, builder);
      builder.append(": ");
    }
    expression.toString(params, builder);
    builder.append(' ').append(kind);

    // canonical order, FILTER last: its condition must not swallow an option that follows it when the text is parsed again
    switch (kind) {
    case RANGES -> {
      builder.append(" (");
      for (int i = 0; i < ranges.size(); i++) {
        if (i > 0)
          builder.append(", ");
        ranges.get(i).toString(params, builder);
      }
      builder.append(')');
      if (defaultLabel != null) {
        builder.append(" DEFAULT ");
        appendQuotedStringLiteral(builder, defaultLabel);
      }
    }
    case HISTOGRAM -> {
      builder.append(Boolean.TRUE.equals(calendar) ? " CALENDAR " : " FIXED ");
      if (interval instanceof String string)
        appendQuotedStringLiteral(builder, string);
      else
        FacetRangeItem.appendBound(params, builder, interval);
    }
    default -> {
    }
    }

    if (top != null) {
      builder.append(" TOP ");
      top.toString(params, builder);
    }
    if (minCount != null) {
      builder.append(" MIN_COUNT ");
      minCount.toString(params, builder);
    }
    if (orderByCount != null) {
      builder.append(orderByCount ? " ORDER BY COUNT" : " ORDER BY VALUE");
      if (ascending != null)
        builder.append(ascending ? " ASC" : " DESC");
    }
    if (include != null) {
      builder.append(" INCLUDE [");
      for (int i = 0; i < include.size(); i++) {
        if (i > 0)
          builder.append(", ");
        include.get(i).toString(params, builder);
      }
      builder.append(']');
    }
    if (missing)
      builder.append(" MISSING");
    if (timezone != null) {
      builder.append(" TIMEZONE ");
      appendQuotedStringLiteral(builder, timezone);
    }
    if (offset != null) {
      builder.append(" OFFSET ");
      appendQuotedStringLiteral(builder, offset);
    }
    if (kind == Kind.HISTOGRAM && (boundsFrom != null || boundsTo != null)) {
      builder.append(" BOUNDS [");
      FacetRangeItem.appendBound(params, builder, boundsFrom);
      builder.append(", ");
      FacetRangeItem.appendBound(params, builder, boundsTo);
      builder.append(']');
    }
    if (filter != null) {
      builder.append(" FILTER ");
      filter.toString(params, builder);
    }
  }

  @Override
  public FacetItem copy() {
    final FacetItem result = new FacetItem();
    result.alias = alias == null ? null : alias.copy();
    result.expression = expression == null ? null : expression.copy();
    result.kind = kind;
    result.top = top == null ? null : top.copy();
    result.minCount = minCount == null ? null : minCount.copy();
    result.filter = filter == null ? null : filter.copy();
    if (include != null) {
      result.include = new ArrayList<>(include.size());
      for (final Expression e : include)
        result.include.add(e.copy());
    }
    result.orderByCount = orderByCount;
    result.ascending = ascending;
    result.missing = missing;
    if (ranges != null) {
      result.ranges = new ArrayList<>(ranges.size());
      for (final FacetRangeItem r : ranges)
        result.ranges.add(r.copy());
    }
    result.defaultLabel = defaultLabel;
    result.calendar = calendar;
    result.interval = interval;
    result.timezone = timezone;
    result.offset = offset;
    result.boundsFrom = FacetRangeItem.copyBound(boundsFrom);
    result.boundsTo = FacetRangeItem.copyBound(boundsTo);
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { alias, expression, kind, top, minCount, filter, include, orderByCount, ascending, missing, ranges,
        defaultLabel, calendar, interval, timezone, offset, boundsFrom, boundsTo };
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    final int includes = include == null ? 0 : include.size();
    final SimpleNode[] elements = new SimpleNode[2 + includes];
    elements[0] = expression;
    elements[1] = filter;
    for (int i = 0; i < includes; i++)
      elements[2 + i] = include.get(i);
    return elements;
  }

  @Override
  public Map<String, Object> toJSON() {
    final Map<String, Object> json = super.toJSON();
    json.put("name", getName());
    json.put("expression", expression.toString());
    json.put("kind", kind.name());
    if (top != null)
      json.put("top", top.getValue());
    if (minCount != null)
      json.put("minCount", minCount.getValue());
    if (filter != null)
      json.put("filter", filter.toString());
    if (include != null) {
      final List<Object> values = new ArrayList<>(include.size());
      for (final Expression e : include)
        values.add(e.toString());
      json.put("include", values);
    }
    if (orderByCount != null)
      json.put("orderBy", orderByCount ? "COUNT" : "VALUE");
    if (ascending != null)
      json.put("ascending", ascending);
    if (missing)
      json.put("missing", true);
    if (ranges != null) {
      final List<Object> values = new ArrayList<>(ranges.size());
      for (final FacetRangeItem r : ranges)
        values.add(r.toJSON());
      json.put("ranges", values);
    }
    if (defaultLabel != null)
      json.put("default", defaultLabel);
    if (calendar != null)
      json.put("calendar", calendar);
    if (interval != null)
      json.put("interval", interval);
    if (timezone != null)
      json.put("timezone", timezone);
    if (offset != null)
      json.put("offset", offset);
    if (boundsFrom != null || boundsTo != null) {
      json.put("boundsFrom", FacetRangeItem.boundToJSON(boundsFrom));
      json.put("boundsTo", FacetRangeItem.boundToJSON(boundsTo));
    }
    return json;
  }
}
