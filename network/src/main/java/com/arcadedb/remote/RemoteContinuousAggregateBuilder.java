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

import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.ContinuousAggregateBuilder;

/**
 * The {@link ContinuousAggregateBuilder} a {@link RemoteSchema} hands out: it accumulates exactly the same state as the
 * embedded builder and, at {@code create()}, renders that state as a single {@code CREATE CONTINUOUS AGGREGATE} and
 * issues it through the server (issue #7688). A caller therefore writes ONE body of builder code and runs it against
 * either kind of {@code Schema}.
 * <p>
 * The checks that need only the builder state run here, in the base class, with the same
 * {@link IllegalArgumentException} as embedded. Every check that needs the schema - the source type exists and is a
 * TimeSeries type, the query has an aliased {@code ts.timeBucket()} and a {@code GROUP BY} - runs on the server,
 * through the same embedded builder, so it refuses the same states with the same sentence, and the remote client maps
 * the server's {@code SchemaException} back to a {@code SchemaException}.
 */
public class RemoteContinuousAggregateBuilder extends ContinuousAggregateBuilder {
  private final RemoteDatabase remoteDatabase;
  private final RemoteSchema   schema;

  RemoteContinuousAggregateBuilder(final RemoteDatabase remoteDatabase, final RemoteSchema schema) {
    super(remoteDatabase);
    this.remoteDatabase = remoteDatabase;
    this.schema = schema;
  }

  @Override
  public ContinuousAggregate create() {
    final String sql = toSQL();

    // Invalidated in a finally for the same reason as RemoteMaterializedViewBuilder: the aggregate comes with a backing
    // type, and a command the server applied but whose response was lost must not leave the type cache stale.
    try {
      remoteDatabase.command("sql", sql);
    } finally {
      schema.invalidateSchema();
    }

    // Read back from the server, so the caller gets what the server derived from the query - bucket interval, bucket
    // alias, timestamp column, source type - rather than a local guess at it.
    return schema.getContinuousAggregate(getName());
  }
}
