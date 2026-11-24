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

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Map;

/**
 * A text that holds no statement: an empty text, a lone {@code ;}, or only comments such as {@code -- ping}. It runs nothing and answers an empty result, as an empty query does in PostgreSQL, where a client uses it to ping a connection;
 * the Postgres wire maps it to {@code EmptyQueryResponse}.
 * <p>
 * Only the {@code parse} rule yields it (issue #13380). A script ({@code parseScript}) already accepts any number of statements, none included.
 */
public class EmptyStatement extends SimpleExecStatement {
  public EmptyStatement() {
  }

  @Override
  public ResultSet executeSimple(final CommandContext context) {
    return new InternalResultSet();
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    // nothing: the statement is the empty text, which parses back to this statement
  }

  @Override
  public EmptyStatement copy() {
    return new EmptyStatement();
  }

  /** Reads and writes nothing, so it is a query as much as a command. */
  @Override
  protected boolean isIdempotentItself() {
    return true;
  }

  @Override
  public boolean equals(final Object o) {
    return this == o || o != null && getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return 0;
  }
}
