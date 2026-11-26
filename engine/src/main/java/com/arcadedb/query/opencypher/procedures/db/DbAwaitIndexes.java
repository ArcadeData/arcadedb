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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.List;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: {@code db.awaitIndexes([timeoutSeconds])}
 * <p>
 * Neo4j builds an index in the background, so a tool that creates one waits for it with this procedure. An ArcadeDB index is complete
 * when {@code CREATE INDEX} returns, so there is nothing to wait for: the procedure accepts the optional timeout and returns no rows.
 * </p>
 */
public class DbAwaitIndexes implements CypherProcedure {
  public static final String NAME = "db.awaitIndexes";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Returns once every index is online; an index is online when its CREATE INDEX returns (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of();
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    return Stream.empty();
  }
}
