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
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.WALFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Assertions;

import java.util.UUID;
import java.util.logging.Level;

public class ReplicationSpeedQuorumMajorityIT extends BasePerformanceTest {
  public static void main(final String[] args) {
    new ReplicationSpeedQuorumMajorityIT().run();
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS.setValue("2424-2500");
    GlobalConfiguration.HA_QUORUM.setValue("majority");
  }

  protected int getTxs() {
    return 100;
  }

  protected int getVerticesPerTx() {
    return 10000;
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  public void run() {
    deleteDatabaseFolders();

    final int parallel = 4;

    databases = new Database[getServerCount()];
    for (int i = 0; i < getServerCount(); ++i) {
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValue("./target/databases");
      databases[i] = new DatabaseFactory(getDatabasePath(i)).create();
    }

    final Database database = getDatabase(0);
    database.transaction(() -> {
      if (isPopulateDatabase()) {
        Assertions.assertFalse(database.getSchema().existsType("Device"));

        VertexType v = database.getSchema().createVertexType("Device", parallel);

        v.createProperty("id", String.class);
        v.createProperty("lastModifiedUserId", String.class);
        v.createProperty("createdDate", String.class);
        v.createProperty("assocJointClosureId", String.class);
        v.createProperty("HolderSpec_Name", String.class);
        v.createProperty("number", String.class);
        v.createProperty("relativeName", String.class);
        v.createProperty("Name", String.class);
        v.createProperty("holderGroupName", String.class);
        v.createProperty("slot2slottype", String.class);
        v.createProperty("inventoryStatus", String.class);
        v.createProperty("lastModifiedDate", String.class);
        v.createProperty("createdUserId", String.class);
        v.createProperty("orientation", String.class);
        v.createProperty("operationalStatus", String.class);
        v.createProperty("supplierName", String.class);

        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "id" }, 2 * 1024 * 1024);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "number" }, 2 * 1024 * 1024);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Device", new String[] { "relativeName" }, 2 * 1024 * 1024);
      }
    });

    // CLOSE ALL DATABASES BEFORE TO START THE SERVERS
    LogManager.instance().log(this, Level.INFO, "TEST: Closing databases before starting");
    for (int i = 0; i < databases.length; ++i) {
      databases[i].close();
      databases[i] = null;
    }

    startServers();

    Database db = getServerDatabase(0, getDatabaseName());

//    db.begin();
//    db.setWALFlush(WALFile.FLUSH_TYPE.YES_NO_METADATA);

    LogManager.instance().log(this, Level.INFO, "TEST: Executing %s transactions with %d vertices each...", null, getTxs(), getVerticesPerTx());

    final int totalToInsert = getTxs() * getVerticesPerTx();
    long counter = 0;

    final long startTimer = System.currentTimeMillis();
    long lastLap = startTimer;
    long lastLapCounter = 0;

    db.setReadYourWrites(false);
    db.async().setCommitEvery(getVerticesPerTx());
    db.async().setParallelLevel(parallel);
    db.async().setTransactionUseWAL(true);
    db.async().setTransactionSync(WALFile.FLUSH_TYPE.YES_NOMETADATA);

    for (int tx = 0; tx < getTxs(); ++tx) {
      for (int i = 0; i < getVerticesPerTx(); ++i) {
        final MutableVertex v = db.newVertex("Device");

        ++counter;

        final String randomString = UUID.randomUUID().toString();

        v.set("id", randomString); // INDEXED
        v.set("number", "" + counter); // INDEXED
        v.set("relativeName", "/shelf=" + counter + "/slot=1"); // INDEXED

        v.set("lastModifiedUserId", "Holder");
        v.set("createdDate", "2011-09-12 14:50:57.0");
        v.set("assocJointClosureId", "434746");
        v.set("HolderSpec_Name", "Slot");
        v.set("Name", "1");
        v.set("holderGroupName", "TBC");
        v.set("slot2slottype", "1900000012");
        v.set("inventoryStatus", "INI");
        v.set("lastModifiedDate", "2011-09-12 14:54:13.0");
        v.set("createdUserId", "Holder");
        v.set("orientation", "NA");
        v.set("operationalStatus", "NotAvailable");
        v.set("supplierName", "TBD");

        db.async().createRecord(v, null);
        //v.save();

        if (counter % 1000 == 0) {
          if (System.currentTimeMillis() - lastLap > 1000) {
            LogManager.instance().log(this, Level.INFO, "TEST: - Progress %d/%d (%d records/sec)", null, counter, totalToInsert, counter - lastLapCounter);
            lastLap = System.currentTimeMillis();
            lastLapCounter = counter;
          }
        }
      }

//      db.commit();
//
//      db.begin();
//      db.setWALFlush(WALFile.FLUSH_TYPE.YES_NO_METADATA);
    }

    db.async().waitCompletion();

    LogManager.instance().log(this, Level.INFO, "Done");

    //Assertions.assertEquals(totalToInsert, db.countType("Device", true), "Check for vertex count for server" + 0);

    db.close();

    //endTest();
    stopServers();
  }

}
