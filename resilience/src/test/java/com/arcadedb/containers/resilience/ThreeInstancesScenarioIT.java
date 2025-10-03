package com.arcadedb.containers.resilience;

import com.arcadedb.test.support.ContainersTestTemplate;
import com.arcadedb.test.support.DatabaseWrapper;
import com.arcadedb.test.support.ServerWrapper;
import eu.rekawek.toxiproxy.Proxy;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ThreeInstancesScenarioIT extends ContainersTestTemplate {

//  @AfterEach
//  @Override
//  public void tearDown() {
//    stopContainers();
//    logger.info("Comparing databases ");
//    DatabaseFactory databaseFactory1 = new DatabaseFactory("./target/databases/arcade1/ha-test");
//    Database db1 = databaseFactory1.open(ComponentFile.MODE.READ_ONLY);
//    DatabaseFactory databaseFactory2 = new DatabaseFactory("./target/databases/arcade2/ha-test");
//    Database db2 = databaseFactory2.open(ComponentFile.MODE.READ_ONLY);
//    DatabaseFactory databaseFactory3 = new DatabaseFactory("./target/databases/arcade3/ha-test");
//    Database db3 = databaseFactory3.open(ComponentFile.MODE.READ_ONLY);
//
//    new DatabaseComparator().compare(db1, db2);
//    new DatabaseComparator().compare(db1, db3);
//    new DatabaseComparator().compare(db2, db3);
//
//    db1.close();
//    db2.close();
//    db3.close();
//
//    databaseFactory1.close();
//    databaseFactory2.close();
//    databaseFactory3.close();
//    logger.info("Databases compared");
//
//  }

  @Test
  @DisplayName("Test resync after network crash with 3 servers in HA mode: one leader and two replicas")
  void oneLeaderAndTwoReplicas() throws IOException {

    logger.info("Creating a proxy for each arcade container");
    final Proxy arcade1Proxy = toxiproxyClient.createProxy("arcade1Proxy", "0.0.0.0:8666", "arcade1:2424");
    final Proxy arcade2Proxy = toxiproxyClient.createProxy("arcade2Proxy", "0.0.0.0:8667", "arcade2:2424");
    final Proxy arcade3Proxy = toxiproxyClient.createProxy("arcade3Proxy", "0.0.0.0:8668", "arcade3:2424");

    logger.info("Creating 3 arcade containers");
    GenericContainer<?> arcade1 = createArcadeContainer("arcade1", "{arcade2}proxy:8667,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade2 = createArcadeContainer("arcade2", "{arcade1}proxy:8666,{arcade3}proxy:8668", "majority", "any",
        network);
    GenericContainer<?> arcade3 = createArcadeContainer("arcade3", "{arcade1}proxy:8666,{arcade2}proxy:8667", "majority", "any",
        network);

    logger.info("Starting the containers in sequence: arcade1 will be the leader");
    List<ServerWrapper> servers = startContainers();

    DatabaseWrapper db1 = new DatabaseWrapper(servers.getFirst(), idSupplier);
    DatabaseWrapper db2 = new DatabaseWrapper(servers.get(1), idSupplier);
    DatabaseWrapper db3 = new DatabaseWrapper(servers.get(2), idSupplier);
    logger.info("Creating the database on arcade server 1");
    db1.createDatabase();

    logger.info("Creating schema on database 1");
    db1.createSchema();

    logger.info("Checking that the database schema is replicated");
    db1.checkSchema();
    db2.checkSchema();
    db3.checkSchema();

    logger.info("Adding data to databases 1");
    db1.addUserAndPhotos(10, 10);
    logger.info("Adding data to databases 2");
    db2.addUserAndPhotos(10, 10);
    logger.info("Adding data to databases 3");
    db3.addUserAndPhotos(10, 10);

    logger.info("Check that all the data are replicated on each instance");
    db1.assertThatUserCountIs(30);
    db2.assertThatUserCountIs(30);
    db3.assertThatUserCountIs(30);
    db1.assertThatPhotoCountIs(300);
    db2.assertThatPhotoCountIs(300);
    db3.assertThatPhotoCountIs(300);

//    logger.info("Disconnecting arcade1 form others");
//    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
//    arcade1Proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);

    logger.info("Adding data to arcade2");
    db2.addUserAndPhotos(100, 10);

    logger.info("Check that all the data are replicated only on arcade1 and arcade2");
    db1.assertThatUserCountIs(130);
    db2.assertThatUserCountIs(130);
    db3.assertThatUserCountIs(130);

//    logger.info("Reconnecting arcade3 ");
//    arcade1Proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
//    arcade1Proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();

    logger.info("Adding data to database");
    db1.addUserAndPhotos(100, 10);

    logger.info("Check that all the data are replicated on each instance");
    db1.assertThatUserCountIs(230);
    db2.assertThatUserCountIs(230);

    logger.info("Waiting for resync");
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> {
          try {
            Long users1 = db1.countUsers();
            Long photos1 = db1.countPhotos();
            Long users2 = db2.countUsers();
            Long photos2 = db2.countPhotos();
            Long users3 = db3.countUsers();
            Long photos3 = db3.countPhotos();
            logger.info("Users:: {} --> {} --> {} - Photos:: {} --> {} --> {}  ", users1, users2, users3, photos1, photos2,
                photos3);
            return users2.equals(users1) && photos2.equals(photos1) && users3.equals(users1) && photos3.equals(photos1);
          } catch (Exception e) {
            return false;
          }
        });

    db1.close();
    db2.close();
    db3.close();

  }

}
