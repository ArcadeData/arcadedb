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
package com.arcadedb.postgres;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.DocumentType;

import java.util.List;
import java.util.Map;

public class PostgresPortal {
  public String                    query;
  public String                    language;
  public Statement                 sqlStatement;
  public List<Long>                parameterTypes;
  public List<Integer>             parameterFormats;
  public List<Object>              parameterValues;
  public List<Integer>             resultFormats;
  public List<Result>              cachedResultSet;
  public Map<String, PostgresType> columns;
  public boolean                   ignoreExecution      = false;
  public boolean                   isExpectingResult;
  public boolean                   executed             = false;
  public boolean                   rowDescriptionSent   = false;
  /**
   * Memoizes {@code PostgresNetworkExecutor.resolveQueryTargetType(sqlStatement)} (issue #6447): a portal can
   * be described and executed - possibly executed repeatedly, for a cursor-based fetch with a LIMIT - several
   * times over its lifetime, and the schema type its FROM target names does not change between them.
   */
  public DocumentType               queryTargetType;
  public boolean                    queryTargetTypeResolved = false;
  /**
   * True when the query is about the emulated system catalog and could not be answered at Parse time because
   * its filters are bound parameters, whose values only arrive with the Bind message (issue #6412).
   */
  public boolean                   catalogQuery         = false;
  /**
   * The complete materialized result of this portal's statement (issue #6458), set once - by whichever of a
   * Describe('P') or the first Execute runs the statement first - and read by every Execute after that to
   * hand out {@code limit}-sized slices via {@link #resultCursor}. {@link #cachedResultSet} holds only the
   * current slice (what the in-flight Execute is about to write to the wire), matching what every existing
   * reader of that field already expects; this field is what makes a second slice possible without re-running
   * the statement or losing the rows a Describe already had to materialize to discover the row's columns.
   */
  public List<Result>              fullResultSet;
  /**
   * How many rows of {@link #fullResultSet} have already been handed to the client across every Execute so
   * far (issue #6458). The next Execute's slice starts here.
   */
  public int                       resultCursor         = 0;
  /**
   * True when the most recently computed slice of {@link #fullResultSet} stopped because Execute's row-limit
   * was hit while rows remained - i.e. the wire must send PortalSuspended for that slice, not CommandComplete.
   * The protocol allows exactly one of the two (issue #6458).
   */
  public boolean                   suspended            = false;

  public PostgresPortal(final String query, String language) {
    this.query = query;
    this.language = language;
    this.isExpectingResult = true;
  }

  @Override
  public String toString() {
    return query;
  }
}
