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
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;
import java.util.logging.*;

/**
 * Created by luigidellaquila on 06/02/15.
 */
public class OrderByItem {
  public static final String   ASC  = "ASC";
  public static final String   DESC = "DESC";
  protected           String   alias;
  protected           Modifier modifier;
  protected           String   recordAttr;
  protected           Rid      rid;
  protected           String   type = ASC;

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getRecordAttr() {
    return recordAttr;
  }

  public void setRecordAttr(String recordAttr) {
    this.recordAttr = recordAttr;
  }

  public Rid getRid() {
    return rid;
  }

  public void setRid(Rid rid) {
    this.rid = rid;
  }

  public void toString(Map<String, Object> params, StringBuilder builder) {

    if (alias != null) {
      builder.append(alias);
      if (modifier != null) {
        modifier.toString(params, builder);
      }
    } else if (recordAttr != null) {
      builder.append(recordAttr);
    } else if (rid != null) {
      rid.toString(params, builder);
    }
    if (type != null) {
      builder.append(" " + type);
    }
  }

  public int compare(Result a, Result b, CommandContext ctx) {
    Object aVal = null;
    Object bVal = null;
    if (rid != null) {
      throw new UnsupportedOperationException("ORDER BY " + rid + " is not supported yet");
    }

    int result = 0;
    if (recordAttr != null) {
      aVal = a.getProperty(recordAttr);
      if (aVal == null) {
        if (recordAttr.equalsIgnoreCase("@rid")) {
          aVal = a.getIdentity().orElse(null);
        } //TODO check other attributes
      }
      bVal = b.getProperty(recordAttr);
      if (bVal == null) {
        if (recordAttr.equalsIgnoreCase("@rid")) {
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
      aVal = modifier.execute(a, aVal, ctx);
      bVal = modifier.execute(b, bVal, ctx);
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
      } catch (Exception e) {
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
    OrderByItem result = new OrderByItem();
    result.alias = alias;
    result.modifier = modifier == null ? null : modifier.copy();
    result.recordAttr = recordAttr;
    result.rid = rid == null ? null : rid.copy();
    result.type = type;
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (modifier != null) {
      modifier.extractSubQueries(collector);
    }
  }

  public boolean refersToParent() {
    if (alias != null && alias.equalsIgnoreCase("$parent")) {
      return true;
    }
    return modifier != null && modifier.refersToParent();
  }

  public Modifier getModifier() {
    return modifier;
  }

  public Result serialize() {
    ResultInternal result = new ResultInternal();
    result.setProperty("alias", alias);
    if (modifier != null) {
      result.setProperty("modifier", modifier.serialize());
    }
    result.setProperty("recordAttr", recordAttr);
    if (rid != null) {
      result.setProperty("rid", rid.serialize());
    }
    result.setProperty("type", type);
    return result;
  }

  public void deserialize(Result fromResult) {
    alias = fromResult.getProperty("alias");
    if (fromResult.getProperty("modifier") != null) {
      modifier = new Modifier(-1);
      modifier.deserialize(fromResult.getProperty("modifier"));
    }
    recordAttr = fromResult.getProperty("recordAttr");
    if (fromResult.getProperty("rid") != null) {
      rid = new Rid(-1);
      rid.deserialize(fromResult.getProperty("rid"));
    }
    type = DESC.equals(fromResult.getProperty("type")) ? DESC : ASC;
  }

  public void setModifier(Modifier modifier) {
    this.modifier = modifier;
  }
}
