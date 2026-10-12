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
import java.util.Map;

/**
 * Returns the property names of a map, a document or a result. The pair of {@link SQLMethodValues}: both ask the receiver
 * for its {@code getPropertyNames()}, which is what keeps the n-th key naming the n-th value.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodKeys extends AbstractSQLMethodKeysValues {
  public static final String NAME = "keys";

  public SQLMethodKeys() {
    super(NAME);
  }

  @Override
  protected Object fromMap(final Map<?, ?> map) {
    return map.keySet();
  }

  @Override
  protected Object fromDocument(final Document document) {
    return new ArrayList<>(document.getPropertyNames());
  }

  @Override
  protected Object fromResult(final Result result) {
    return result.getPropertyNames();
  }
}
