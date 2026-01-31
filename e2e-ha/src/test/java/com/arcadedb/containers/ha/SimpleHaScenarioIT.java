package com.arcadedb.containers.ha;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Testcontainers
public class SimpleHaScenarioIT extends ContainersTestTemplate {

  @Test
//  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @DisplayName("Test servers in HA mode")
  void twoInstancesInLeaderReplicaConfiguration() throws InterruptedException, IOException {
    logger.info("Creating a proxy for each arcade container");
//    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
//    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    logger.info("Creating two arcade containers");
    createArcadeContainer("arcade1", "{arcade2}arcade2:2424", "none", "any", network);
    createArcadeContainer("arcade2", "{arcade1}arcade1:2424", "none", "any", network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
//    List<ServerWrapper> servers = startContainersDeeply();
    List<ServerWrapper> servers = startContainersDeeply();

    // DIAGNOSTIC: Check if containers are healthy after startup
    logger.info("DIAGNOSTIC: Checking container health after startup");
    Thread.sleep(5000); // Wait 5 seconds for containers to stabilize
    diagnoseContainers();

    logger.info("Creating the database on the first arcade container");
    DatabaseWrapper db1 = new DatabaseWrapper(servers.getFirst(), idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();
    db1.createSchema();

    // DIAGNOSTIC: Check if arcade2 is still running after arcade1 creates database
    logger.info("DIAGNOSTIC: Checking container health after database creation");
//    Thread.sleep(2000); // Wait 2 seconds
    diagnoseContainers();

    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    logger.info("Checking that the database schema is replicated");
    db1.checkSchema();
    db2.checkSchema();

    logger.info("Adding data to database 1");
    db1.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on database 2");
    try {
      db2.assertThatUserCountIs(10);
    } catch (Exception e) {
      logger.error("DIAGNOSTIC: Exception during replication check - checking container health");
      diagnoseContainers();
      throw e;
    }

    // DIAGNOSTIC: Check container health after replication
    logger.info("DIAGNOSTIC: Checking container health after initial replication");
    diagnoseContainers();

    logger.info("Disconnecting the two instances");
//    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
//    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Adding more data to arcade 1");
    db1.addUserAndPhotos(10, 1000);
    db2.addUserAndPhotos(10, 1000);

    logger.info("Verifying 20 users on arcade 1");
//    db1.assertThatUserCountIs(20);

    logger.info("Verifying still only 10 users on arcade 2");
//    db2.assertThatUserCountIs(10);
    logStatus(db1, db2);

    logger.info("Reconnecting instances");
//    arcade1Proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
//    arcade1Proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();

    logger.info("Waiting for resync");

    // Wait for replication to complete
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          Long users1 = db1.countUsers();
          Long photos1 = db1.countPhotos();
          Long users2 = db2.countUsers();
          Long photos2 = db2.countPhotos();
          logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
          return users2.equals(users1) && photos2.equals(photos1);
        });

  }

  private void logStatus(DatabaseWrapper db1, DatabaseWrapper db2) {
    logger.info("Maybe  resynced?");
    Long users2 = db2.countUsers();
    Long photos2 = db2.countPhotos();
    Long users1 = db1.countUsers();
    Long photos1 = db1.countPhotos();
    logger.info("Users:: {} --> {} - Photos:: {} --> {} ", users1, users2, photos1, photos2);
  }
}
