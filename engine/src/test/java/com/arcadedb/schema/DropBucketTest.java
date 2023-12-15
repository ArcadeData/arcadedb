package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DropBucketTest extends TestHelper {

  @Test
  public void createDropCluster() {
    database.getSchema().createBucket("test");
    Assertions.assertNotNull(database.getSchema().getBucketByName("test"));
    database.getSchema().dropBucket("test");
    Assertions.assertFalse(database.getSchema().existsBucket("test"));
  }
}
