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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.parser.FieldMatchPathItem;
import com.arcadedb.query.sql.parser.MatchPathItem;

import java.util.Collections;
import java.util.Iterator;

public class MatchFieldTraverser extends MatchEdgeTraverser {
  public MatchFieldTraverser(Result lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  public MatchFieldTraverser(Result lastUpstreamRecord, MatchPathItem item) {
    super(lastUpstreamRecord, item);
  }

  protected Iterable<ResultInternal> traversePatternEdge(Identifiable startingPoint, CommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      String alias = getEndpointAlias();
      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      // TODO check possible results!
      qR = ((FieldMatchPathItem) this.item).getExp().execute(startingPoint, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return Collections.EMPTY_LIST;
    }
    if (qR instanceof Identifiable) {
      return Collections.singleton(new ResultInternal((Document) ((Identifiable) qR).getRecord()));
    }
    if (qR instanceof Iterable) {
      final Iterator<Object> iter = ((Iterable) qR).iterator();

      return () -> new Iterator<ResultInternal>() {
        private ResultInternal nextElement;

        @Override
        public boolean hasNext() {
          if (nextElement == null) {
            fetchNext();
          }
          return nextElement != null;
        }

        @Override
        public ResultInternal next() {
          if (nextElement == null) {
            fetchNext();
          }
          if (nextElement == null) {
            throw new IllegalStateException();
          }
          ResultInternal res = nextElement;
          nextElement = null;
          return res;
        }

        public void fetchNext() {
          while (iter.hasNext()) {
            Object o = iter.next();
            if (o instanceof Identifiable) {
              nextElement = new ResultInternal((Identifiable) o);
              break;
            } else if (o instanceof ResultInternal) {
              nextElement = (ResultInternal) o;
              break;
            } else if (o == null) {
              continue;
            } else {
              throw new UnsupportedOperationException();
            }
          }
        }
      };
    }
    return Collections.EMPTY_LIST;
  }
}
