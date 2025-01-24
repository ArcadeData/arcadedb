package com.arcadedb.database;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseComparatorTest extends TestHelper {

  @Test
  void compareDatabaseWithItself() {

    DatabaseComparator comparator = new DatabaseComparator();

    comparator.compare(database, database);


  }
}
