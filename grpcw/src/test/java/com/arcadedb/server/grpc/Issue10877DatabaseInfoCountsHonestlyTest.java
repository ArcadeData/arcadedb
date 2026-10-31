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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import com.arcadedb.utility.FileUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10877: {@code getDatabaseInfo} computed each of its numbers inside a {@code catch (Throwable ignore)}, so a type it could
 * not count was left out of the record total without a word, and a schema it could not read was reported as zero classes. The
 * total now covers the types it could count and the answer names the ones it could not, and the census of the types is the
 * schema's own, read back at assertion time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10877DatabaseInfoCountsHonestlyTest extends BaseGraphServerTest {
  private static final String NAME = "Issue10877Info";

  private ArcadeDbGrpcAdminService service;
  private ServerDatabase           database;

  @BeforeEach
  void createDatabase() {
    service = new ArcadeDbGrpcAdminService(getServer(0), new DefaultCredentialsValidator());
    database = getServer(0).createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    database.getSchema().createDocumentType("Doc");
    database.getSchema().createVertexType("Person");
    database.command("sql", "CREATE TIMESERIES TYPE Metric TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO Doc SET k = ?", i);
      database.command("sql", "INSERT INTO Person SET name = 'a'");
      database.command("sql", "INSERT INTO Metric SET ts = 1000, host = 'h', v = 1");
      database.command("sql", "INSERT INTO Metric SET ts = 2000, host = 'h', v = 2");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (getServer(0).existsDatabase(NAME))
      getServer(0).dropLocalDatabase(getServer(0).getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(
        new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  /** Every type of the schema is counted, and the numbers are the schema's. */
  @Test
  void theAnswerIsTheCensusOfTheSchema() {
    final GetDatabaseInfoResponse info = databaseInfo();

    assertThat(info.getClasses()).isEqualTo(database.getSchema().getTypes().size());
    assertThat(info.getIndexes()).isEqualTo(database.getSchema().getIndexes().length);
    assertThat(info.getType()).isEqualTo("graph");
    assertThat(info.getRecords()).isEqualTo(countEveryType(database)).isEqualTo(6L);
    assertThat(info.getUncountedTypesList()).isEmpty();
  }

  /**
   * The report's non-racy cause: a TimeSeries type whose engine is not available cannot be counted. It is named in the answer
   * and the total is the sum of the others, rather than a smaller number handed over as the database's.
   */
  @Test
  void aTypeThatCannotBeCountedIsNamedNotDropped() {
    final LocalTimeSeriesType metric = (LocalTimeSeriesType) database.getSchema().getType("Metric");
    // what a failed initEngine() leaves (issue #6356)
    metric.close();
    assertThat(metric.isEngineAvailable()).as("the state this test is about").isFalse();

    final GetDatabaseInfoResponse info = databaseInfo();

    assertThat(info.getUncountedTypesList()).containsExactly("Metric");
    assertThat(info.getClasses()).as("the type is still a type of the database").isEqualTo(database.getSchema().getTypes().size());
    assertThat(info.getRecords()).as("the types that could be counted").isEqualTo(4L);

    // Every countable type is represented explicitly; do not derive the expected set from the loop's own iteration count.
    final List<String> counted = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (!info.getUncountedTypesList().contains(type.getName()))
        counted.add(type.getName());
    assertThat(counted).containsExactlyInAnyOrder("Doc", "Person");
  }

  private GetDatabaseInfoResponse databaseInfo() {
    final CollectingObserver<GetDatabaseInfoResponse> observer = new CollectingObserver<>();
    service.getDatabaseInfo(GetDatabaseInfoRequest.newBuilder().setCredentials(
        DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build()).setName(NAME).build(), observer);
    assertThat(observer.error).as("the call failed").isNull();
    assertThat(observer.completed).isTrue();
    assertThat(observer.values).hasSize(1);
    return observer.values.getFirst();
  }

  private static long countEveryType(final Database database) {
    long total = 0;
    for (final DocumentType type : database.getSchema().getTypes())
      total += database.countType(type.getName(), false);
    return total;
  }

  private static final class CollectingObserver<T> implements StreamObserver<T> {
    private final List<T> values = new ArrayList<>();
    private Throwable     error;
    private boolean       completed;

    @Override
    public void onNext(final T value) {
      values.add(value);
    }

    @Override
    public void onError(final Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }
  }
}
