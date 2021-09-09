/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.server.distributed;

// import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;

/** Insert records concurrently against the cluster */
public abstract class AbstractDistributedWriteTest extends AbstractServerClusterTest {
  protected final int delayWriter = 0;
  protected int writerCount = 5;
  protected volatile int count = 100;
  protected CountDownLatch runningWriters;
  //  protected final OPartitionedDatabasePoolFactory poolFactory = new
  // OPartitionedDatabasePoolFactory();

  class Writer implements Callable<Void> {
    private int serverId;
    private int threadId;

    public Writer(final int iServerId, final int iThreadId) {
      serverId = iServerId;
      threadId = iThreadId;
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(threadId);
      for (int i = 0; i < count; i++) {
        final ODatabaseDocumentInternal database = getDatabase(serverId);

        try {
          if ((i + 1) % 100 == 0)
            System.out.println(
                "\nWriter "
                    + threadId
                    + "("
                    + database.getURL()
                    + ") managed "
                    + (i + 1)
                    + "/"
                    + count
                    + " records so far");

          final ODocument person = createRecord(database, i);
          updateRecord(database, i);
          checkRecord(database, i);
          checkIndex(database, (String) person.field("name"), person.getIdentity());

          if (delayWriter > 0) Thread.sleep(delayWriter);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + database.getURL());
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          System.out.println("Writer received exception (db=" + database.getURL());
          e.printStackTrace();
          break;
        } finally {
          database.close();
          runningWriters.countDown();
        }
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }

    private ODocument createRecord(ODatabaseDocument database, int i) {
      final String uniqueId = serverId + "-" + threadId + "-" + i;

      ODocument person =
          new ODocument("Person")
              .fields(
                  "id",
                  UUID.randomUUID().toString(),
                  "name",
                  "Billy" + uniqueId,
                  "surname",
                  "Mayes" + uniqueId,
                  "birthday",
                  new Date(),
                  "children",
                  uniqueId);
      database.save(person);

      Assert.assertTrue(person.getIdentity().isPersistent());

      return person;
    }

    private void updateRecord(ODatabaseDocument database, int i) {
      ODocument doc = loadRecord(database, i);
      doc.field("updated", true);
      doc.save();
    }

    private void checkRecord(ODatabaseDocument database, int i) {
      ODocument doc = loadRecord(database, i);
      Assert.assertEquals(doc.field("updated"), Boolean.TRUE);
    }

    private void checkIndex(ODatabaseDocumentInternal database, final String key, final ORID rid) {
      final OIndex index =
          database.getSharedContext().getIndexManager().getIndex(database, "Person.name");
      final List<ORID> rids;
      try (Stream<ORID> stream = index.getInternal().getRids(key)) {
        rids = stream.collect(Collectors.toList());
      }

      Assert.assertEquals(1, rids.size());
      Assert.assertTrue(rids.contains(rid));
    }

    private ODocument loadRecord(ODatabaseDocument database, int i) {
      final String uniqueId = serverId + "-" + threadId + "-" + i;

      List<ODocument> result =
          database.query(
              new OSQLSynchQuery<ODocument>(
                  "select from Person where name = 'Billy" + uniqueId + "'"));
      if (result.size() == 0)
        Assert.assertTrue("No record found with name = 'Billy" + uniqueId + "'!", false);
      else if (result.size() > 1)
        Assert.assertTrue(
            result.size() + " records found with name = 'Billy" + uniqueId + "'!", false);

      return result.get(0);
    }
  }

  public String getDatabaseName() {
    return getClass().getSimpleName();
  }

  @Override
  public void executeTest() throws Exception {

    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutors = Executors.newCachedThreadPool();

    runningWriters = new CountDownLatch(serverInstance.size() * writerCount);

    int serverId = 0;
    int threadId = 0;
    List<Callable<Void>> writerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : serverInstance) {
      for (int j = 0; j < writerCount; j++) {
        Callable writer = createWriter(serverId, threadId++, server);
        writerWorkers.add(writer);
      }
      serverId++;
    }
    List<Future<Void>> futures = writerExecutors.invokeAll(writerWorkers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    writerExecutors.shutdown();
    Assert.assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished, shutting down readers");
  }

  protected abstract String getDatabaseURL(ServerRun server);

  protected Callable<Void> createWriter(
      final int serverId, final int threadId, final ServerRun serverRun) {
    return new Writer(serverId, threadId);
  }

  protected void dumpDistributedDatabaseCfgOfAllTheServers() {
    for (ServerRun s : serverInstance) {
      final ODistributedServerManager dManager = s.getServerInstance().getDistributedManager();
      final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(getDatabaseName());
      final String cfgOutput = "";

      ODistributedServerLog.info(
          this,
          s.getServerInstance().getDistributedManager().getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Distributed configuration for database: %s (version=%d)%s\n",
          getDatabaseName(),
          cfg.getVersion(),
          cfgOutput);
    }
  }

  protected void checkThePersonClassIsPresentOnAllTheServers() {
    for (ServerRun s : serverInstance) {
      // CHECK THE CLASS WAS CREATED ON ALL THE SERVERS
      ODatabaseDocument database = getDatabase(s);
      try {
        org.junit.Assert.assertTrue(
            "Server=" + s + " db=" + getDatabaseName(),
            database.getMetadata().getSchema().existsClass("Person"));
      } finally {
        database.close();
      }
    }
  }
}
