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

import com.arcadedb.database.Database;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: {@code db.info()}
 * <p>
 * Returns one row about the database the statement runs on: {@code id}, {@code name} and {@code creationDate}, which is how a client
 * asks for the name of the database it is connected to (issue #13543).
 * </p>
 */
public class DbInfo implements CypherProcedure {
  public static final String NAME = "db.info";

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
    return 0;
  }

  @Override
  public String getDescription() {
    return "Provides information regarding the database (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("id", "name", "creationDate");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final List<Object> values = describe(context.getDatabase());
    final ResultInternal result = new ResultInternal();
    final List<String> fields = getYieldFields();
    for (int i = 0; i < fields.size(); i++)
      result.setProperty(fields.get(i), values.get(i));
    return Stream.of(result);
  }

  /**
   * The values of the {@link #getYieldFields() fields} for {@code database}: its name (the identifier of a database in a server is
   * its name), and the time its directory was created as an ISO-8601 instant, empty when the file system does not tell.
   */
  public static List<Object> describe(final Database database) {
    String creationDate = "";
    try {
      final String path = database.getDatabasePath();
      if (path != null)
        creationDate = Files.readAttributes(new File(path).toPath(), BasicFileAttributes.class).creationTime().toInstant().toString();
    } catch (final IOException | RuntimeException e) {
      // a store with no directory (a remote or in-memory one) has no creation date to report
    }
    return List.of(database.getName(), database.getName(), creationDate);
  }
}
