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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectCompiled {
  private final Select select;

  public SelectCompiled(final Select select) {
    this.select = select;
  }

  public SelectCompiled parameter(final String paramName, final Object paramValue) {
    if (select.parameters == null)
      select.parameters = new HashMap<>();
    select.parameters.put(paramName, paramValue);
    return this;
  }

  public JSONObject json() {
    final JSONObject json = new JSONObject();

    if (select.fromType != null) {
      json.put("fromType", select.fromType.getName());
      if (!select.polymorphic)
        json.put("polymorphic", select.polymorphic);
    } else if (select.fromBuckets != null)
      json.put("fromBuckets", select.fromBuckets.stream().map(b -> b.getName()).collect(Collectors.toList()));

    if (select.rootTreeElement != null)
      json.put("where", select.rootTreeElement.toJSON());

    if (select.limit > -1)
      json.put("limit", select.limit);
    if (select.skip > -1)
      json.put("skip", select.skip);
    if (select.timeoutInMs > 0) {
      json.put("timeoutInMs", select.timeoutInMs);
      json.put("exceptionOnTimeout", select.exceptionOnTimeout);
    }

    return json;
  }

  public SelectCompiled parallel() {
    select.parallel = true;
    return this;
  }

  public SelectIterator<Vertex> vertices() {
    return select.vertices();
  }

  public SelectIterator<Edge> edges() {
    return select.edges();
  }

  public SelectIterator<Document> documents() {
    return select.documents();
  }

  public long count() {
    return select.count();
  }

  public boolean exists() {
    return select.exists();
  }

  public Stream<Document> stream() {
    return select.stream();
  }
}
