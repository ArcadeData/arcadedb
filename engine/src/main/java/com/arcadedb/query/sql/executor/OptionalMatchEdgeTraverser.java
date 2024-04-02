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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;

import java.util.*;

/**
 * Created by luigidellaquila on 17/10/16.
 */
public class OptionalMatchEdgeTraverser extends MatchEdgeTraverser {
  public static final Result EMPTY_OPTIONAL = new ResultInternal((Database) null);

  public OptionalMatchEdgeTraverser(final Result lastUpstreamRecord, final EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected void init(final CommandContext context) {
    if (downstream == null) {
      super.init(context);
      if (!downstream.hasNext()) {
        final List x = new ArrayList();
        x.add(EMPTY_OPTIONAL);
        downstream = x.iterator();
      }
    }
  }

  public Result next(final CommandContext context) {
    init(context);
    if (!downstream.hasNext()) {
      throw new NoSuchElementException();
    }

    final String endPointAlias = getEndpointAlias();
    final Object prevValue = sourceRecord.getProperty(endPointAlias);
    final ResultInternal next = downstream.next();

    if (isEmptyOptional(prevValue)) {
      return sourceRecord;
    }
    if (!isEmptyOptional(next)) {
      if (prevValue != null && !equals(prevValue, next.getElement().get())) {
        return null;
      }
    }

    final ResultInternal result = new ResultInternal(context.getDatabase());
    for (final String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, next.getElement().map(x -> toResult(x)).orElse(null));
    return result;
  }

  public static boolean isEmptyOptional(final Object elem) {
    if (elem == EMPTY_OPTIONAL) {
      return true;
    }
    return elem instanceof Result && EMPTY_OPTIONAL == ((Result) elem).getElement().orElse(null);

  }
}
