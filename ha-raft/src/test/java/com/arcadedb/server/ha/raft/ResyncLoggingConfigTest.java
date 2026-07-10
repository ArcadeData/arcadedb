package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResyncLoggingConfigTest {

  @Test
  void defaultsAreSane() {
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING.getValueAsBoolean()).isTrue();
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL.getValueAsLong()).isEqualTo(5000L);
    assertThat(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD.getValueAsLong()).isEqualTo(10000L);
    assertThat(GlobalConfiguration.HA_RESYNC_CATCHUP_LAG_THRESHOLD.getValueAsLong()).isEqualTo(1000L);
  }

  @Test
  void keysFollowHaNamespace() {
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_LOGGING.getKey()).isEqualTo("arcadedb.ha.resyncProgressLogging");
    assertThat(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL.getKey()).isEqualTo("arcadedb.ha.resyncProgressInterval");
    assertThat(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD.getKey()).isEqualTo("arcadedb.ha.peerUnreachableThreshold");
    assertThat(GlobalConfiguration.HA_RESYNC_CATCHUP_LAG_THRESHOLD.getKey()).isEqualTo("arcadedb.ha.resyncCatchupLagThreshold");
  }
}
