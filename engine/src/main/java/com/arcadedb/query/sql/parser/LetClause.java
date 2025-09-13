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
import java.util.stream.*;

public class LetClause extends SimpleNode {

  protected List<LetItem> items = new ArrayList<LetItem>();

  public LetClause(final int id) {
    super(id);
  }

  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("LET ");
    boolean first = true;
    for (final LetItem item : items) {
      if (!first) {
        builder.append(", ");
      }
      item.toString(params, builder);
      first = false;
    }
  }

  public void addItem(final LetItem item) {
    this.items.add(item);
  }

  public LetClause copy() {
    final LetClause result = new LetClause(-1);
    result.items = items.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  public List<LetItem> getItems() {
    return items;
  }

  @Override
  protected Object[] getIdentityElements() {
    return getCacheableElements();
  }

  public void extractSubQueries(final SubQueryCollector collector) {
    for (final LetItem item : items) {
      item.extractSubQueries(collector);
    }
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return items.toArray(new LetItem[items.size()]);
  }
}

/* JavaCC - OriginalChecksum=201a864b5ed7f1fbe0533843a7acd03d (do not edit this line) */
