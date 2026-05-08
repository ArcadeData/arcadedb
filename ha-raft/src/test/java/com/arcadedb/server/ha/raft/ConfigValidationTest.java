package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigValidationTest {

  @Test
  void haReplicationLagWarningHasDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getValueAsLong()).isEqualTo(1000L);
  }

  @Test
  void haRaftPortDefaultIs2434() {
    assertThat(GlobalConfiguration.HA_RAFT_PORT.getValueAsInteger()).isEqualTo(2434);
  }

  /**
   * Without this override Ratis falls back to its 64MB stock default, which is too small for
   * legitimate bulk-load batches (a 50k-vertex GraphBatch can produce a single ~75MB Raft entry).
   * The customer-reported bug was triggered by exactly that mismatch.
   */
  @Test
  void grpcMessageSizeMaxDefaultIs128MB() {
    assertThat(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX.getValueAsLong())
        .isEqualTo(128L * 1024 * 1024);
  }

  @Test
  void raftPropertiesBuilderAppliesGrpcMessageSizeMax() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX, 200L * 1024 * 1024);
    final RaftProperties props = RaftPropertiesBuilder.build(cfg);
    final SizeInBytes size = GrpcConfigKeys.messageSizeMax(props, msg -> { /* swallow */ });
    assertThat(size.getSize()).isEqualTo(200L * 1024 * 1024);
  }
}
