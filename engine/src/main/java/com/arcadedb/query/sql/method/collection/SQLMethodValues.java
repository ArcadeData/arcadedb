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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Returns the values of a map, a document or a result, in the order {@link SQLMethodKeys} lists their names.
 * <p>
 * For a document or a result the names come from {@code getPropertyNames()} - the same question {@code keys()} asks - and
 * each value is read for its name. Collecting the values off {@code toMap()} instead let metadata ({@code @cat},
 * {@code @type}, {@code @rid}, an edge's {@code @in} and {@code @out}) in as unlabelled values and, on a row that
 * overrides a property of its element, answered the element's value where the row has another (issues #6472 and #9843).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodValues extends AbstractSQLMethodKeysValues {

  public static final String NAME = "values";

  public SQLMethodValues() {
    super(NAME);
  }

  @Override
  protected Object fromMap(final Map<?, ?> map) {
    return map.values();
  }

  @Override
  protected Object fromDocument(final Document document) {
    // toMap(false) excludes the metadata fields (@rid, @type, @cat) and is the one place a document's values are converted
    // (embedded documents and lists become maps and lists), exactly as values() has always answered them (#6472). It is one
    // map per call, as before; the lookup by name is what keeps each value in the slot of its key
    final Map<String, Object> properties = document.toMap(false);
    return valuesOf(document.getPropertyNames(), properties::get);
  }

  @Override
  protected Object fromResult(final Result result) {
    return valuesOf(result.getPropertyNames(), result::getProperty);
  }

  private static List<Object> valuesOf(final Set<String> names, final Function<String, Object> valueOf) {
    final List<Object> values = new ArrayList<>(names.size());
    for (final String name : names)
      values.add(valueOf.apply(name));
    return values;
  }
}
