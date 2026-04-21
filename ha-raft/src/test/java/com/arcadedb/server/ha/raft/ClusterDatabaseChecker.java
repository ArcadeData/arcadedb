package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparatorErrorsReporter;
import com.arcadedb.database.DatabaseFactory;

public class ClusterDatabaseChecker {

  public static void main(String[] args) {

    DatabaseComparatorErrorsReporter comparator = new DatabaseComparatorErrorsReporter();

    DatabaseFactory factory = new DatabaseFactory(
        "/Users/frank/projects/arcade/worktrees/ha-redesign/adb1/databases/playwithpictures");
    Database db1 = factory.open();

    DatabaseFactory factory2 = new DatabaseFactory(
        "/Users/frank/projects/arcade/worktrees/ha-redesign/adb2/databases/playwithpictures");
    Database db2 = factory2.open();

    DatabaseFactory factory3 = new DatabaseFactory(
        "/Users/frank/projects/arcade/worktrees/ha-redesign/adb3/databases/playwithpictures");
    Database db3 = factory3.open();

    System.out.println(db1.getDatabasePath() + "<-->" + db2.getDatabasePath());
    System.out.println(comparator.compare(db1, db2));
    comparator.resetCollector();
    System.out.println("--------");
    System.out.println(db1.getDatabasePath() + "<-->" + db3.getDatabasePath());
    System.out.println(comparator.compare(db1, db3));
    comparator.resetCollector();
    System.out.println("--------");
    System.out.println(db2.getDatabasePath() + "<-->" + db3.getDatabasePath());
    System.out.println(comparator.compare(db2, db3));

    System.exit(0);
  }
}
