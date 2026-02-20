package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigValidationTest {

  @Test
  void haImplementationDefaultsToLegacy() {
    assertThat(GlobalConfiguration.HA_IMPLEMENTATION.getValueAsString()).isEqualTo("legacy");
  }

  @Test
  void haReplicationLagWarningHasDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getValueAsLong()).isEqualTo(1000L);
  }
}
