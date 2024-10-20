package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DropBucketTest extends TestHelper {

  @Test
  public void createDropCluster() {
    database.getSchema().createBucket("test");
    assertThat(database.getSchema().getBucketByName("test")).isNotNull();
    database.getSchema().dropBucket("test");
    assertThat(database.getSchema().existsBucket("test")).isFalse();
  }
}
