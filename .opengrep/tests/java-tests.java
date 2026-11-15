package com.arcadedb.opengrep;

import java.util.concurrent.TimeUnit;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.Suite;
import org.junit.jupiter.api.Tag;

// ruleid: test-uses-junit-assertions
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

class WallClockShapes {
  static final long HANG_MS = 10_000L;

  void elapsedInline(final long start) {
    // ruleid: test-asserts-raw-wall-clock
    assertThat(System.currentTimeMillis() - start).isLessThan(2_000L);
  }

  void clockAgainstADeadline(final long deadline) {
    // ruleid: test-asserts-raw-wall-clock
    assertThat(System.nanoTime()).as("x").isLessThan(deadline);
  }

  void elapsedThroughALocal(final long start) {
    // ruleid: test-asserts-raw-wall-clock
    final long elapsed = System.currentTimeMillis() - start;
    doWork();
    assertThat(elapsed).as("x").isLessThan(1_000L);
  }

  void elapsedConverted(final long start) {
    // ruleid: test-asserts-raw-wall-clock
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(HANG_MS / 2);
  }

  void lowerBound(final long elapsed, final long timeoutMs) {
    // ok: test-asserts-raw-wall-clock
    assertThat(elapsed).isGreaterThanOrEqualTo(timeoutMs);
  }

  void clockOnTheBoundSide(final long ts) {
    // ok: test-asserts-raw-wall-clock
    assertThat(ts).isLessThan(System.currentTimeMillis() + 1000);
  }

  void stallAware() {
    // ok: test-asserts-raw-wall-clock
    StallAwareStopwatch.start().assertGaveUpWithin(2_000L, "x");
  }

  void doWork() {
  }
}

// ruleid: cucumber-suite-with-junit-tag
@Suite
@IncludeEngines("cucumber")
@Tag("slow")
class TaggedCucumberSuite {
}

// ok: cucumber-suite-with-junit-tag
@Suite
@IncludeEngines("cucumber")
class UntaggedCucumberSuite {
}
