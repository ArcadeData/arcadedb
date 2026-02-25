package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;

public class ClusterDatatbaseChecker {

  public static void main(String[] args) {

    DatabaseComparator comparator = new DatabaseComparator();

    DatabaseFactory factory = new DatabaseFactory("/Users/frank/projects/arcade/worktrees/ha-redesign/adb1/databases/playwithpictures");
    Database db1 = factory.open();

    DatabaseFactory factory2 = new DatabaseFactory("/Users/frank/projects/arcade/worktrees/ha-redesign/adb2/databases/playwithpictures");
    Database db2 = factory2.open();

    DatabaseFactory factory3 = new DatabaseFactory("/Users/frank/projects/arcade/worktrees/ha-redesign/adb3/databases/playwithpictures");
    Database db3 = factory3.open();


    comparator.compare(db1, db2);
    comparator.compare(db1, db3);
    comparator.compare(db2, db3);

    System.out.println("\nDB1, DB2 and DB3 are identical");
    System.exit(0);
  }
}
