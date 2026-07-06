package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSetStatisticsTest {
  @Test
  void defaultResultSetHasNoStatistics() {
    final IteratorResultSet rs = new IteratorResultSet(Collections.emptyIterator());
    assertThat(rs.getStatistics()).isEmpty();
  }

  @Test
  void attachedStatisticsAreReturned() {
    final QueryStatistics stats = new QueryStatistics();
    stats.incNodesCreated();
    final IteratorResultSet rs = new IteratorResultSet(Collections.emptyIterator());
    rs.setStatistics(stats);
    assertThat(rs.getStatistics()).isPresent();
    assertThat(rs.getStatistics().get().getNodesCreated()).isEqualTo(1);
  }
}
