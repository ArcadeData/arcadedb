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
package com.arcadedb;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public abstract class TestHelper {
  protected static final int             PARALLEL_LEVEL = 4;
  protected final        DatabaseFactory factory;
  protected              Database        database;
  protected              boolean         autoStartTx    = false;

  public interface DatabaseTest<PAR> {
    void call(PAR iArgument) throws Exception;
  }

  protected TestHelper() {
    this(true);
  }

  protected TestHelper(final boolean cleanBeforeTest) {
    GlobalConfiguration.PROFILE.setValue(getPerformanceProfile());

    checkActiveDatabases();

    if (cleanBeforeTest)
      FileUtils.deleteRecursively(new File(getDatabasePath()));
    factory = new DatabaseFactory(getDatabasePath());
    database = factory.exists() ? factory.open() : factory.create();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);

    if (autoStartTx)
      database.begin();
  }

  protected boolean isCheckingDatabaseIntegrity() {
    return true;
  }

  public static void executeInNewDatabase(final DatabaseTest<Database> callback) throws Exception {
    executeInNewDatabase(UUID.randomUUID().toString(), callback);
  }

  public static DocumentType createRandomType(final Database database) {
    Exception lastException = null;
    for (int i = 0; i < 3; i++) {
      try {
        return database.getSchema().createDocumentType("RandomType" + ThreadLocalRandom.current().nextInt(100_000));
      } catch (Exception e) {
        // RETRY
        lastException = e;
      }
    }
    throw lastException instanceof RuntimeException ? (RuntimeException) lastException : new RuntimeException(lastException);
  }

  public static void executeInNewDatabase(final String testName, final DatabaseTest<Database> callback) throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + testName)) {
      if (factory.exists()) {
        factory.open().drop();
        assertThat(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath())).isNull();
      }

      final Database database = factory.create();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath())).isEqualTo(database);
      try {
        database.begin();
        callback.call(database);
        database.commit();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }
  }

  public static Database createDatabase(final String databaseName) {
    return dropDatabase(databaseName).create();
  }

  public static DatabaseFactory dropDatabase(final String databaseName) {
    final DatabaseFactory factory = new DatabaseFactory(databaseName);
    if (factory.exists())
      factory.open().drop();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(factory.getDatabasePath())).isNull();
    return factory;
  }

  protected void reopenDatabase() {
    if (database != null) {
      database.close();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isNull();
    }
    database = factory.open();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);
  }

  protected void reopenDatabaseInReadOnlyMode() {
    if (database != null) {
      database.close();
      assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isNull();
    }

    database = factory.open(ComponentFile.MODE.READ_ONLY);
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);
  }

  protected String getDatabasePath() {
    return "target/databases/" + getClass().getSimpleName();
  }

  protected void beginTest() {
    // SUB CLASS CAN EXTEND THIS
  }

  protected void endTest() {
    // SUB CLASS CAN EXTEND THIS
  }

  @BeforeEach
  public void beforeTest() {
    GlobalConfiguration.SERVER_ROOT_PATH.setValue("./target");
    if (autoStartTx && !database.isTransactionActive())
      database.begin();
    beginTest();
  }

  @AfterEach
  public void afterTest() {
    endTest();

    if (database.isTransactionActive())
      database.commit();

    if (database != null && database.isOpen()) {
      if (isCheckingDatabaseIntegrity())
        checkDatabaseIntegrity();

      if (database.getMode() == ComponentFile.MODE.READ_ONLY)
        reopenDatabase();

      ((DatabaseInternal) database).getEmbedded().drop();
      database = null;
    }

    checkActiveDatabases();
    FileUtils.deleteRecursively(new File(getDatabasePath()));
    GlobalConfiguration.resetAll();
  }

  @AfterAll
  public static void endAllTests() {
    GlobalConfiguration.resetAll();
  }

  protected String getPerformanceProfile() {
    return "default";
  }

  public static void expectException(final CallableNoReturn callback, final Class<? extends Throwable> expectedException)
      throws Exception {
    try {
      callback.call();
      fail("");
    } catch (final Throwable e) {
      if (e.getClass().equals(expectedException))
        // EXPECTED
        return;

      if (e instanceof Exception exception)
        throw exception;

      throw new Exception(e);
    }
  }

  protected void checkDatabaseIntegrity() {
    final ResultSet result = database.command("sql", "check database");
    while (result.hasNext()) {
      final Result row = result.next();

      assertThat(row.<String>getProperty("operation")).isEqualTo("check database");
      assertThat((Long) row.getProperty("autoFix")).isEqualTo(0);
      assertThat(((Collection<?>) row.getProperty("corruptedRecords")).size()).isEqualTo(0);
      assertThat((Long) row.getProperty("invalidLinks")).isEqualTo(0);
      assertThat(((Collection<?>) row.getProperty("warnings")).size()).as("Warnings" + row.getProperty("warnings")).isEqualTo(0);
    }
  }

  public static void checkActiveDatabases() {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();

    if (!activeDatabases.isEmpty())
      LogManager.instance()
          .log(TestHelper.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced closing...");

    for (final Database db : activeDatabases)
      db.close();

    assertThat(activeDatabases.isEmpty()).as("Found active databases: " + activeDatabases).isTrue();
  }

  /**
   * Overwrites the record-type byte of {@code rid} with a value no {@code RecordFactory} branch knows, so the
   * record still occupies its slot and still has a valid size but cannot be materialised - a corruption shape
   * shared by tests that need a record {@code CHECK DATABASE} can find but not load (e.g. an index-rebuild
   * trigger), as opposed to a missing or truncated one.
   */
  protected static void corruptRecordTypeByte(final DatabaseInternal db, final RID rid) {
    final int fileId = rid.getBucketId();
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
        final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
        final int recordOffset = (int) page.readUnsignedInt(slotOffset);
        assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
        final long[] recordSize = page.readNumberAndSize(recordOffset);
        page.writeByte((int) (recordOffset + recordSize[1]), (byte) 99);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Holds {@code fileIds} locked for {@code holdMillis} via {@code DatabaseInternal.executeLockingFiles} - the same
   * primitive production lock-contention retry paths (e.g. index rebuild) use - on a requester (this thread)
   * distinct from whichever thread started it, to deterministically force a {@code LockTimeoutException} there.
   */
  public static final class LockHoldingThread extends Thread {
    private final DatabaseInternal            db;
    private final List<Integer>               fileIds;
    private final long                        holdMillis;
    public final  CountDownLatch              lockAcquired = new CountDownLatch(1);
    public final  AtomicReference<Throwable>  error        = new AtomicReference<>();

    public LockHoldingThread(final DatabaseInternal db, final List<Integer> fileIds, final long holdMillis) {
      super("lock-holder");
      this.db = db;
      this.fileIds = fileIds;
      this.holdMillis = holdMillis;
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        db.executeLockingFiles(fileIds, () -> {
          lockAcquired.countDown();
          Thread.sleep(holdMillis);
          return null;
        });
      } catch (final Throwable t) {
        error.set(t);
        lockAcquired.countDown();
      }
    }
  }
}
