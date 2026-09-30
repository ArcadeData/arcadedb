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
package com.arcadedb.remote;

import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewBuilder;

/**
 * The {@link MaterializedViewBuilder} a {@link RemoteSchema} hands out: it accumulates exactly the same state as the
 * embedded builder and, at {@code create()}, renders that state as a single {@code CREATE MATERIALIZED VIEW} and issues
 * it through the server (issue #7688). A caller therefore writes ONE body of builder code and runs it against either
 * kind of {@code Schema}.
 * <p>
 * The validation that needs only the builder state lives in the base class, so both paths refuse the same inputs with
 * the same {@link IllegalArgumentException}; {@link MaterializedViewBuilder#toSQL()} additionally refuses a state the
 * grammar cannot express rather than shipping DDL the server would reject.
 * <p>
 * A refusal that needs the schema - a name already taken by a view or a type, a source type that does not exist - comes
 * from the server, which runs the very same embedded builder: same sentence, and the remote client maps the server's
 * {@code SchemaException} back to a {@code SchemaException}, so a caller catches the same type on both paths. Pinned by
 * {@code Issue7688RemoteViewBuildersIT.aDuplicateNameIsRefusedOnBothPathsWithTheSameExceptionType}.
 */
public class RemoteMaterializedViewBuilder extends MaterializedViewBuilder {
  private final RemoteDatabase remoteDatabase;
  private final RemoteSchema   schema;

  RemoteMaterializedViewBuilder(final RemoteDatabase remoteDatabase, final RemoteSchema schema) {
    super(remoteDatabase);
    this.remoteDatabase = remoteDatabase;
    this.schema = schema;
  }

  @Override
  public MaterializedView create() {
    final String sql = toSQL();

    // The cache is invalidated in a finally, not after the command: the view comes with a backing type, and a command
    // that fails AFTER the server applied it - the response is lost on the way back - would otherwise leave this schema
    // instance answering existsType() with false for a type that exists.
    try {
      remoteDatabase.command("sql", sql);
    } finally {
      schema.invalidateSchema();
    }

    // Read back from the server rather than assembled locally, so the caller gets what the server stored: the
    // normalized query, the resolved source types, the query classification and, with IF NOT EXISTS, the view that
    // was already there - the same one an embedded create() returns in that case.
    return schema.getMaterializedView(getName());
  }
}
