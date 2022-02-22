package com.arcadedb.integration;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Assertions;

import java.util.*;
import java.util.logging.*;

public abstract class TestHelper {
  public static void checkActiveDatabases() {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();

    if (!activeDatabases.isEmpty())
      LogManager.instance().log(TestHelper.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced closing...");

    for (Database db : activeDatabases)
      db.close();

    Assertions.assertTrue(activeDatabases.isEmpty(), "Found active databases: " + activeDatabases);
  }
}
