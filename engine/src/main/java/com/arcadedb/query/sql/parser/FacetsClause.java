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
 * The {@code FACETS} clause of the outermost SELECT: the facets computed over every record the WHERE clause matches.
 */
public class FacetsClause extends SimpleNode {
  public List<FacetItem> items = new ArrayList<>();

  public FacetsClause() {
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("FACETS ");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0)
        builder.append(", ");
      items.get(i).toString(params, builder);
    }
  }

  @Override
  public FacetsClause copy() {
    final FacetsClause result = new FacetsClause();
    result.items = new ArrayList<>(items.size());
    for (final FacetItem item : items)
      result.items.add(item.copy());
    return result;
  }

  @Override
  protected Object[] getIdentityElements() {
    return new Object[] { items };
  }

  @Override
  protected SimpleNode[] getCacheableElements() {
    return items.toArray(new SimpleNode[0]);
  }

  @Override
  public Map<String, Object> toJSON() {
    final Map<String, Object> json = super.toJSON();
    final List<Object> itemsJson = new ArrayList<>(items.size());
    for (final FacetItem item : items)
      itemsJson.add(item.toJSON());
    json.put("items", itemsJson);
    return json;
  }
}
