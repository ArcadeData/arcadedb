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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Property;

import java.util.*;
import java.util.stream.*;

public class Json extends SimpleNode {

  protected List<JsonItem> items = new ArrayList<>();

  public Json(final int id) {
    super(id);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("{");
    boolean first = true;
    for (final JsonItem item : items) {
      if (!first) {
        builder.append(", ");
      }
      item.toString(params, builder);

      first = false;
    }
    builder.append("}");
  }

  public Document toDocument(final Identifiable source, final CommandContext context) {
    final String className = getClassNameForDocument(context, source);
    final MutableDocument doc;
    if (className != null) {
      doc = context.getDatabase().newDocument(className);
    } else {
      doc = context.getDatabase().newDocument(null);
    }
    for (final JsonItem item : items) {
      final String name = item.getLeftValue();
      if (name == null) {
        continue;
      }
      final Object value;
      if (item.right.value instanceof Json json) {
        value = json.toDocument(source, context);
      } else {
        value = item.right.execute(source, context);
      }
      doc.set(name, value);
    }

    return doc;
  }

  public Map<String, Object> toMap(final Identifiable source, final CommandContext context) {
    final Map<String, Object> doc = new HashMap<String, Object>();
    for (final JsonItem item : items) {
      final String name = item.getLeftValue();
      if (name == null) {
        continue;
      }
      final Object value = item.right.execute(source, context);
      doc.put(name, value);
    }

    return doc;
  }

  public Map<String, Object> toMap(final Result source, final CommandContext context) {
    final Map<String, Object> doc = new HashMap<String, Object>();
    for (final JsonItem item : items) {
      final String name = item.getLeftValue();
      if (name == null) {
        continue;
      }
      final Object value = item.right.execute(source, context);
      doc.put(name, value);
    }

    return doc;
  }

  private String getClassNameForDocument(final CommandContext context, final Identifiable record) {
    if (record != null) {
      final Document doc = record.asDocument();
      if (doc != null)
        return doc.getTypeName();
    }

    for (final JsonItem item : items) {
      final String left = item.getLeftValue();
      if (left != null && left.toLowerCase(Locale.ENGLISH).equals(Property.TYPE_PROPERTY)) {
        return String.valueOf(item.right.execute((Result) null, context));
      }
    }

    return null;
  }

  public boolean isAggregate(final CommandContext context) {
    for (final JsonItem item : items) {
      if (item.isAggregate(context)) {
        return true;
      }
    }
    return false;
  }

  public Json splitForAggregation(final AggregateProjectionSplit aggregateSplit, final CommandContext context) {
    if (isAggregate(context)) {
      final Json result = new Json(-1);
      for (final JsonItem item : items) {
        result.items.add(item.splitForAggregation(aggregateSplit, context));
      }
      return result;
    } else {
      return this;
    }
  }

  public Json copy() {
    final Json result = new Json(-1);
    result.items = items.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { items };
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    for (final JsonItem item : items) {
      item.extractSubQueries(collector);
    }
  }

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public boolean refersToParent() {
    return false;
  }
}
/* JavaCC - OriginalChecksum=3beec9f6db486de944498588b51a505d (do not edit this line) */
