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

import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;

import java.util.List;

/**
 * The {@link TimeSeriesTypeBuilder} a {@link RemoteSchema} hands out: it accumulates exactly the same state as the
 * embedded builder and, at {@code create()}, renders that state as DDL and issues it through the server (issue
 * #7399).
 * <p>
 * The point of the exercise is that a caller writes ONE body of builder code and runs it against either kind of
 * {@code Schema}. That only holds if the two agree on what a valid builder is, which is why the validation lives in
 * the base class and why {@link TimeSeriesTypeBuilder#toSQL()} refuses a state the grammar cannot express rather
 * than shipping DDL the server would reject.
 * <p>
 * The rendered DDL is a single statement (issue #7689), so the type the server ends up with is either the whole
 * declaration the builder accumulated or nothing at all.
 * <p>
 * <b>Where the two do NOT agree is the exception type for a name that is already taken.</b> Both refuse it, but the
 * embedded {@code create()} answers with {@code SchemaException("Type 'X' already exists")} before touching storage,
 * while this one has no client-side existence check - the rendered DDL carries no {@code IF NOT EXISTS} and no
 * check would be race-free anyway - so the refusal comes back from the server as a {@link RemoteException} wrapping
 * the {@code CommandExecutionException}, with the same sentence in its message. A caller that catches
 * {@code SchemaException} specifically for "already exists" therefore has to catch both. Measured, and pinned by
 * {@code Issue7399RemoteTimeSeriesTypeBuilderIT.aDuplicateTypeNameIsRefusedOnBothPathsWithDifferentExceptionTypes}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteTimeSeriesTypeBuilder extends TimeSeriesTypeBuilder {
  private final RemoteDatabase remoteDatabase;
  private final RemoteSchema   schema;

  RemoteTimeSeriesTypeBuilder(final RemoteDatabase remoteDatabase, final RemoteSchema schema) {
    super(remoteDatabase);
    this.remoteDatabase = remoteDatabase;
    this.schema = schema;
  }

  @Override
  public TimeSeriesType create() {
    final List<String> statements = toSQL();

    // toSQL() renders the WHOLE declaration - per-column codecs and downsampling policy included - as a single
    // CREATE TIMESERIES TYPE since issue #7689, so this loop issues exactly one command and a create is all or
    // nothing. It used to issue a CREATE and then an ALTER for the downsampling policy, and a caller whose
    // connection dropped between the two, or who lost UPDATE_SCHEMA in between, was left with a type on the server
    // that had no policy and an exception in hand. The loop stays because toSQL()'s contract is a list executed in
    // order, not because there is more than one statement to execute.
    //
    // The cache is invalidated in a finally, not after the command: a command that fails AFTER the server applied
    // it - the response is lost, the connection drops on the way back - leaves the type there, and leaving the
    // cache untouched would have this schema instance answer existsType() with false for a type that exists.
    try {
      for (final String sql : statements)
        remoteDatabase.command("sql", sql);
    } finally {
      schema.invalidateSchema();
    }

    // The type is read back from the server rather than assembled locally, so what the caller gets is the
    // declaration the server actually stored - the resolved shard count included, which is the server's
    // ASYNC_WORKER_THREADS when the builder named none and is therefore not knowable on this side.
    final DocumentType created = schema.getType(getName());
    if (!(created instanceof TimeSeriesType timeSeriesType))
      throw new SchemaException(
          "Type '" + getName() + "' was created but the server reports it as " + created.getClass().getSimpleName()
              + " rather than a TIMESERIES type");
    return timeSeriesType;
  }
}
