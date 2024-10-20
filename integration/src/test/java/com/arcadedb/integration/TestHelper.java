package com.arcadedb.integration;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;

import java.util.*;
import java.util.logging.*;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class TestHelper {
  public static void checkActiveDatabases() {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();

    if (!activeDatabases.isEmpty())
      LogManager.instance().log(TestHelper.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced closing...");

    for (final Database db : activeDatabases)
      db.close();

    assertThat(activeDatabases.isEmpty()).as("Found active databases: " + activeDatabases).isTrue();
  }
}
