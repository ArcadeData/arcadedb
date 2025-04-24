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

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;
import java.util.logging.*;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * Created by luigidellaquila on 06/02/15.
 */
public class OrderByItem {
  public static final String   ASC  = "ASC";
  public static final String   DESC = "DESC";
  protected           String   alias;
  protected           Modifier modifier;
  protected           String   recordAttr;
  protected           String   type = ASC;

  public String getAlias() {
    return alias;
  }

  public void setAlias(final String alias) {
    this.alias = alias;
  }

  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getRecordAttr() {
    return recordAttr;
  }

  public void setRecordAttr(final String recordAttr) {
    this.recordAttr = recordAttr;
  }

  public String getName() {
    return alias != null ? alias : recordAttr != null ? recordAttr : null;
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    if (alias != null) {
      builder.append(alias);
      if (modifier != null) {
        modifier.toString(params, builder);
      }
    } else if (recordAttr != null) {
      builder.append(recordAttr);
    }
    if (type != null) {
      builder.append(" " + type);
    }
  }

  public int compare(final Result a, final Result b, final CommandContext context) {
    Object aVal = null;
    Object bVal = null;

    int result = 0;
    if (recordAttr != null) {
      aVal = a.getProperty(recordAttr);
      if (aVal == null) {
        if (recordAttr.equalsIgnoreCase(RID_PROPERTY)) {
          aVal = a.getIdentity().orElse(null);
        } //TODO check other attributes
      }
      bVal = b.getProperty(recordAttr);
      if (bVal == null) {
        if (recordAttr.equalsIgnoreCase(RID_PROPERTY)) {
          bVal = b.getIdentity().orElse(null);
        } //TODO check other attributes
      }
    } else if (alias != null) {
      aVal = a.getProperty(alias);
      bVal = b.getProperty(alias);
    }
    if (aVal == null && bVal == null) {
      aVal = a.getMetadata(alias);
      bVal = b.getMetadata(alias);
    }
    if (modifier != null) {
      aVal = modifier.execute(a, aVal, context);
      bVal = modifier.execute(b, bVal, context);
    }
    if (aVal == null) {
      if (bVal == null) {
        result = 0;
      } else {
        result = -1;
      }
    } else if (bVal == null) {
      result = 1;
    } else if (aVal instanceof Comparable && bVal instanceof Comparable) {
      try {
        result = BinaryComparator.compareTo(aVal, bVal);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during comparison", e);
        result = 0;
      }
    }
    if (DESC.equals(type)) {
      result = -1 * result;
    }
    return result;
  }

  public OrderByItem copy() {
    final OrderByItem result = new OrderByItem();
    result.alias = alias;
    result.modifier = modifier == null ? null : modifier.copy();
    result.recordAttr = recordAttr;
    result.type = type;
    return result;
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    if (modifier != null)
      modifier.extractSubQueries(collector);
  }

  public boolean refersToParent() {
    if (alias != null && alias.equalsIgnoreCase("$parent"))
      return true;

    return modifier != null && modifier.refersToParent();
  }

  public Modifier getModifier() {
    return modifier;
  }

  public void setModifier(final Modifier modifier) {
    this.modifier = modifier;
  }
}
