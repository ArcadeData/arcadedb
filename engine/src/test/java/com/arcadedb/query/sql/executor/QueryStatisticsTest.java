package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryStatisticsTest {
  @Test
  void freshInstanceHasNoUpdates() {
    final QueryStatistics s = new QueryStatistics();
    assertThat(s.containsUpdates()).isFalse();
    assertThat(s.getNodesCreated()).isZero();
  }

  @Test
  void incrementsAreTracked() {
    final QueryStatistics s = new QueryStatistics();
    s.incNodesCreated();
    s.incNodesCreated();
    s.incRelationshipsCreated();
    s.addPropertiesSet(3);
    s.addLabelsAdded(2);
    assertThat(s.getNodesCreated()).isEqualTo(2);
    assertThat(s.getRelationshipsCreated()).isEqualTo(1);
    assertThat(s.getPropertiesSet()).isEqualTo(3);
    assertThat(s.getLabelsAdded()).isEqualTo(2);
    assertThat(s.containsUpdates()).isTrue();
  }

  @Test
  void addingZeroPropertiesKeepsNoUpdatesWhenNothingElseChanged() {
    final QueryStatistics s = new QueryStatistics();
    s.addPropertiesSet(0);
    s.addLabelsAdded(0);
    assertThat(s.containsUpdates()).isFalse();
  }
}
