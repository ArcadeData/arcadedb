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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Fixture support for the sealed-store apply tests (issue #11274): a TIMESERIES type draws a random sealed owner id when it is
 * created, and a sealed file written for another owner is refused at install. In a cluster the follower's type carries the
 * leader's owner because the schema is replicated; two databases a test creates independently do not, so the follower fixture
 * has to adopt the leader's owner the way the replicated schema would give it.
 */
final class SealedOwnerFixtures {

  private SealedOwnerFixtures() {
  }

  /**
   * Gives the type {@code typeName} of {@code follower} the sealed owner of the same type of {@code leader}, and returns the
   * reopened follower: the owner is read from the schema at load, so it is rewritten in the schema file and the database reopened.
   * Call it before the follower holds any sealed file.
   */
  static LocalDatabase adoptOwnerOf(final LocalDatabase leader, final LocalDatabase follower, final String typeName)
      throws IOException {
    final long owner = ((LocalTimeSeriesType) leader.getSchema().getType(typeName)).getSealedOwner();
    final String path = follower.getDatabasePath();
    follower.close();

    final Path schemaFile = Path.of(path, LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(Files.readString(schemaFile));
    schema.getJSONObject("types").getJSONObject(typeName).put("sealedOwner", owner);
    Files.writeString(schemaFile, schema.toString());

    // the empty sealed file written at creation records the owner the type drew then, and a file of another owner is refused
    try (final DirectoryStream<Path> sealed = Files.newDirectoryStream(Path.of(path), "*.ts.sealed*")) {
      for (final Path file : sealed)
        Files.delete(file);
    }

    return (LocalDatabase) new DatabaseFactory(path).open();
  }
}
