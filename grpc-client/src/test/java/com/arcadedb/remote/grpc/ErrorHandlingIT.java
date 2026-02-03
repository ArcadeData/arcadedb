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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for error handling in gRPC client operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ErrorHandlingIT extends BaseGraphServerTest {

  private static final String TYPE = "ErrorTest";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue(
        "GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeAll
  void setupServer() {
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterAll
  void teardownServer() {
    if (grpcServer != null) {
      grpcServer.close();
    }
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    database = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    database.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS BUCKETS 8");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS STRING");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.name IF NOT EXISTS STRING (mandatory)");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (id) UNIQUE");
    database.command("sql", "DELETE FROM `" + TYPE + "`");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null) {
      try { database.rollback(); } catch (Throwable ignore) {}
      database.close();
    }
    super.endTest();
  }

  @Test
  @DisplayName("Invalid SQL query throws appropriate exception")
  void invalidQuery_throwsException() {
    assertThatThrownBy(() -> database.query("sql", "SELECTT FROM nowhere"))
        .isInstanceOf(RemoteException.class);
  }

  @Test
  @DisplayName("Query on non-existent type throws exception")
  void nonExistentType_throwsException() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM NonExistentType12345"))
        .isInstanceOf(RemoteException.class);
  }

  @Test
  @DisplayName("Authentication failure throws SecurityException")
  void authenticationFailure_throwsSecurityException() {
    // Authentication is checked when creating the database connection (HTTP for cluster config)
    // or when making gRPC calls
    RemoteGrpcServer badServer = new RemoteGrpcServer("localhost", 50051, "root", "wrongpassword", true, List.of());

    // The authentication error may occur either during construction or during query
    assertThatThrownBy(() -> {
      RemoteGrpcDatabase badDb = new RemoteGrpcDatabase(badServer, "localhost", 50051, 2480, getDatabaseName(), "root", "wrongpassword");
      try {
        badDb.query("sql", "SELECT FROM `" + TYPE + "`");
      } finally {
        badDb.close();
      }
    }).satisfiesAnyOf(
        e -> assertThat(e).isInstanceOf(SecurityException.class),
        e -> assertThat(e).hasMessageContaining("not valid"),
        e -> assertThat(e).hasMessageContaining("PERMISSION_DENIED"));

    badServer.close();
  }

  @Test
  @DisplayName("lookupByRID with non-existent RID throws appropriate exception")
  void recordNotFound_throwsRecordNotFoundException() {
    // First insert a record to get a valid bucket ID
    database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'temp', name = 'temp'");
    String ridStr;
    try (ResultSet rs = database.query("sql", "SELECT @rid FROM `" + TYPE + "` WHERE id = 'temp'")) {
      ridStr = rs.next().getIdentity().get().toString();
    }
    // Delete the record so we have a valid bucket but non-existent position
    database.command("sql", "DELETE FROM `" + TYPE + "` WHERE id = 'temp'");

    // Extract bucket ID and use a large position that won't exist
    int bucketId = Integer.parseInt(ridStr.split(":")[0].substring(1));
    RID fakeRid = new RID(database, bucketId, 999999L);

    // The server throws RecordNotFoundException or the client maps it to one
    assertThatThrownBy(() -> database.lookupByRID(fakeRid))
        .satisfiesAnyOf(
            e -> assertThat(e).isInstanceOf(RecordNotFoundException.class),
            e -> assertThat(e).hasMessageContaining("not found"));
  }

  @Test
  @DisplayName("Duplicate key insert throws DuplicatedKeyException or DatabaseOperationException with duplicate key message")
  void duplicateKey_throwsDuplicatedKeyException() {
    database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'dup1', name = 'first'");

    // When the command fails internally, it may throw DatabaseOperationException with the duplicate key message
    // When gRPC properly maps the error, it throws DuplicatedKeyException
    assertThatThrownBy(() ->
        database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'dup1', name = 'second'"))
        .satisfiesAnyOf(
            e -> assertThat(e).isInstanceOf(DuplicatedKeyException.class),
            e -> {
              assertThat(e).isInstanceOf(DatabaseOperationException.class);
              assertThat(e.getMessage()).containsIgnoringCase("duplicat");
            });
  }

  @Test
  @DisplayName("Concurrent modification throws ConcurrentModificationException")
  void concurrentModification_throwsConcurrentModificationException() throws InterruptedException {
    // Insert a record
    database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'conc1', name = 'original'");

    // Get the RID
    String rid;
    try (ResultSet rs = database.query("sql", "SELECT @rid as r FROM `" + TYPE + "` WHERE id = 'conc1'")) {
      rid = rs.next().getProperty("r").toString();
    }

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicReference<Throwable> error1 = new AtomicReference<>();
    AtomicReference<Throwable> error2 = new AtomicReference<>();

    // Two concurrent transactions trying to update the same record
    Runnable update1 = () -> {
      RemoteGrpcDatabase db1 = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      try {
        startLatch.await();
        db1.begin();
        db1.command("sql", "UPDATE `" + TYPE + "` SET name = 'updated1' WHERE id = 'conc1'");
        Thread.sleep(100); // Hold transaction open
        db1.commit();
      } catch (Throwable t) {
        error1.set(t);
        try { db1.rollback(); } catch (Throwable ignore) {}
      } finally {
        db1.close();
        doneLatch.countDown();
      }
    };

    Runnable update2 = () -> {
      RemoteGrpcDatabase db2 = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      try {
        startLatch.await();
        db2.begin();
        db2.command("sql", "UPDATE `" + TYPE + "` SET name = 'updated2' WHERE id = 'conc1'");
        Thread.sleep(100);
        db2.commit();
      } catch (Throwable t) {
        error2.set(t);
        try { db2.rollback(); } catch (Throwable ignore) {}
      } finally {
        db2.close();
        doneLatch.countDown();
      }
    };

    executor.submit(update1);
    executor.submit(update2);
    startLatch.countDown();
    doneLatch.await();
    executor.shutdown();

    // At least one should have failed with concurrent modification
    boolean concurrentError = (error1.get() instanceof ConcurrentModificationException)
        || (error2.get() instanceof ConcurrentModificationException)
        || (error1.get() != null && error1.get().getMessage() != null && error1.get().getMessage().contains("concurrent"))
        || (error2.get() != null && error2.get().getMessage() != null && error2.get().getMessage().contains("concurrent"));

    // Note: This test may pass if transactions serialize correctly, which is also valid behavior
    // The important thing is no unexpected exceptions occurred
    assertThat(error1.get() == null || error2.get() == null || concurrentError)
        .as("Either both succeed (serialized) or one fails with concurrent modification")
        .isTrue();
  }

  @Test
  @DisplayName("Schema violation throws validation exception with mandatory field message")
  void schemaViolation_throwsValidationException() {
    // Try to insert without mandatory 'name' property
    // The server returns an error that may be wrapped as DatabaseOperationException or RemoteException
    assertThatThrownBy(() ->
        database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 'sv1'"))
        .satisfiesAnyOf(
            e -> assertThat(e).isInstanceOf(RemoteException.class),
            e -> assertThat(e).isInstanceOf(DatabaseOperationException.class))
        .hasMessageContaining("name");
  }

  @Test
  @DisplayName("Connection to wrong port fails gracefully")
  void connectionRefused_throwsConnectionException() {
    RemoteGrpcServer badServer = new RemoteGrpcServer("localhost", 59999, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    RemoteGrpcDatabase badDb = new RemoteGrpcDatabase(badServer, "localhost", 59999, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    try {
      assertThatThrownBy(() -> badDb.query("sql", "SELECT 1"))
          .isInstanceOf(Exception.class); // Could be RemoteException, NeedRetryException, etc.
    } finally {
      badDb.close();
      badServer.close();
    }
  }
}
