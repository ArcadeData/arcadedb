package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ha.raft.FollowerResyncProgressTracker.Event;
import com.arcadedb.server.ha.raft.FollowerResyncProgressTracker.Tick;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FollowerResyncProgressTrackerTest {

  @Test
  void notBehindProducesNothing() {
    final FollowerResyncProgressTracker t = new FollowerResyncProgressTracker(5000L);
    assertThat(t.onTick(1000L, 1000L, 0L).event()).isEqualTo(Event.NONE);
  }

  @Test
  void emitsStartProgressFinishAcrossCatchUp() {
    final FollowerResyncProgressTracker t = new FollowerResyncProgressTracker(5000L);

    final Tick start = t.onTick(900L, 1000L, 0L);
    assertThat(start.event()).isEqualTo(Event.STARTED);
    assertThat(start.message()).contains("mode=catch-up").contains("100 entries behind");

    // Within the progress interval: no progress line.
    assertThat(t.onTick(920L, 1000L, 1000L).event()).isEqualTo(Event.NONE);

    // After the interval, still behind: progress line.
    final Tick prog = t.onTick(960L, 1000L, 6000L);
    assertThat(prog.event()).isEqualTo(Event.PROGRESS);
    assertThat(prog.message()).contains("catch-up").contains("960/1000");

    // Caught up: finished line, once.
    final Tick done = t.onTick(1000L, 1000L, 7000L);
    assertThat(done.event()).isEqualTo(Event.FINISHED);
    assertThat(done.message()).contains("mode=catch-up").contains("result=ok");

    // Idle afterwards: nothing.
    assertThat(t.onTick(1000L, 1000L, 8000L).event()).isEqualTo(Event.NONE);
  }

  @Test
  void negativeIndexIsIgnoredWithoutStateChange() {
    final FollowerResyncProgressTracker t = new FollowerResyncProgressTracker(5000L);
    assertThat(t.onTick(-1L, 1000L, 0L).event()).isEqualTo(Event.NONE);
    // A real "behind" tick still starts cleanly afterwards.
    assertThat(t.onTick(900L, 1000L, 1000L).event()).isEqualTo(Event.STARTED);
  }
}
