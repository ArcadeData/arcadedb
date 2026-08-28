/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb;

import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6875, a follow-up to #6837. Two defects that compound.
 * <p>
 * The writers ({@code set_server_setting} and its HTTP twin) stored the caller's raw string in the
 * {@link ContextConfiguration} map without checking it against {@link GlobalConfiguration#getType()}, so an
 * unparseable value was accepted with a success envelope and blew up later, inside whichever component read
 * the setting next.
 * <p>
 * Underneath that, the two accessors for the same setting did not agree on what parses:
 * {@link GlobalConfiguration#getValueAsInteger()} coerces a non-{@code Number} through
 * {@link FileUtils#getSizeAsNumber(Object)} (so {@code "1MB"} reads), while
 * {@link ContextConfiguration#getValueAsInteger(GlobalConfiguration)} used {@code Integer.parseInt} (so the
 * same stored string threw). A validator written against either one alone would have been wrong for the
 * other, which is why the accessors are reconciled here first and the coercion helper built on top of it.
 * <p>
 * These tests hold a {@link ContextConfiguration} of their own rather than mutating the JVM-wide
 * {@link GlobalConfiguration} values, except where the global accessor itself is under test - those cases
 * restore the previous value in a {@code finally}.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6875SettingValueCoercionTest {

  /**
   * The bug, at the accessor level: a raw string reaching the context map (from a writer, from
   * {@code fromJSON}, from the {@code Map} constructor) had to read back the same through both accessors.
   * Before the fix this threw {@code NumberFormatException} on the context path while the global path
   * returned 1048576 for the very same text.
   */
  @Test
  void bothAccessorsAgreeOnASizeSuffixedIntegralValue() {
    final ContextConfiguration ctx = new ContextConfiguration();
    ctx.setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getKey(), "1MB");

    assertThat(ctx.getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT)).isEqualTo(1024L * 1024L);
    assertThat(ctx.getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT))
        .isEqualTo(FileUtils.getSizeAsNumber("1MB"));

    final ContextConfiguration intCtx = new ContextConfiguration();
    intCtx.setValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey(), "1KB");
    assertThat(intCtx.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(1024);
  }

  /** Whitespace around a value is tolerated identically on both paths - the HTTP twin used to leave a leading space. */
  @Test
  void bothAccessorsTolerateSurroundingWhitespace() {
    final ContextConfiguration ctx = new ContextConfiguration();
    ctx.setValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey(), " 7 ");
    assertThat(ctx.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(7);

    final Object previous = GlobalConfiguration.ASYNC_WORKER_THREADS.getValue();
    try {
      GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(" 7 ");
      assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger()).isEqualTo(7);
    } finally {
      GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(previous);
    }
  }

  /** The regression the issue asks for: what {@code coerce} accepts must read back through BOTH accessors. */
  @Test
  void aCoercedValueIsReadableThroughBothAccessors() {
    assertReadableThroughBothAccessors(GlobalConfiguration.ASYNC_WORKER_THREADS, "12");
    assertReadableThroughBothAccessors(GlobalConfiguration.ASYNC_WORKER_THREADS, "1KB");
    assertReadableThroughBothAccessors(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, "7500");
    assertReadableThroughBothAccessors(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, "2MB");
    assertReadableThroughBothAccessors(GlobalConfiguration.SERVER_METRICS_TRACING_SAMPLING_RATE, "0.25");
    assertReadableThroughBothAccessors(GlobalConfiguration.DATE_TIME_FORMAT, "yyyy-MM-dd HH:mm:ss");
  }

  private void assertReadableThroughBothAccessors(final GlobalConfiguration cfg, final String value) {
    final Object coerced = cfg.coerce(value);

    final ContextConfiguration ctx = new ContextConfiguration();
    ctx.setValue(cfg.getKey(), coerced);

    final Object previous = cfg.getValue();
    try {
      cfg.setValue(value);
      if (cfg.getType() == Integer.class)
        assertThat(ctx.getValueAsInteger(cfg)).isEqualTo(cfg.getValueAsInteger());
      else if (cfg.getType() == Long.class)
        assertThat(ctx.getValueAsLong(cfg)).isEqualTo(cfg.getValueAsLong());
      else if (cfg.getType() == Float.class)
        assertThat(ctx.getValueAsFloat(cfg)).isEqualTo(cfg.getValueAsFloat());
      else
        assertThat(ctx.getValueAsString(cfg)).isEqualTo(cfg.getValueAsString());
    } finally {
      cfg.setValue(previous);
    }

    // and the raw string, stored uncoerced the way fromJSON stores it, reads back the same
    final ContextConfiguration rawCtx = new ContextConfiguration();
    rawCtx.setValue(cfg.getKey(), value);
    if (cfg.getType() == Integer.class)
      assertThat(rawCtx.getValueAsInteger(cfg)).isEqualTo(((Number) coerced).intValue());
    else if (cfg.getType() == Long.class)
      assertThat(rawCtx.getValueAsLong(cfg)).isEqualTo(((Number) coerced).longValue());
    else if (cfg.getType() == Float.class)
      assertThat(rawCtx.getValueAsFloat(cfg)).isEqualTo(((Number) coerced).floatValue());
  }

  @Test
  void coerceRejectsAnUnparseableValueForATypedSetting() {
    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerce("abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.asyncWorkerThreads")
        .hasMessageContaining("Integer")
        .hasMessageContaining("abc");

    assertThatThrownBy(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("soon"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.commitLockTimeout");

    assertThatThrownBy(() -> GlobalConfiguration.SERVER_METRICS_TRACING_SAMPLING_RATE.coerce("half"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.serverMetrics.tracing.samplingRate");
  }

  /** An Integer setting cannot hold a value only a Long can represent: truncating it would be a silent wrong answer. */
  @Test
  void coerceRejectsAnIntegralValueOutsideTheIntegerRange() {
    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerce("64GB"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.asyncWorkerThreads");

    // the same magnitude is fine for a Long setting
    assertThat(GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("64GB")).isEqualTo(64L * 1024 * 1024 * 1024);
  }

  /**
   * The bound has to hold for a boxed {@link Number} too, and not only for text parsed by
   * {@code FileUtils.getSizeAsNumber}: {@code Number.intValue()} keeps the low 32 bits, so an out-of-range
   * {@code Long} would be stored silently truncated - where the {@code Integer.parseInt} this replaced threw.
   */
  @Test
  void coerceRejectsAnOutOfRangeNumberAsWellAsAnOutOfRangeString() {
    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerce(5_000_000_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.asyncWorkerThreads");

    // and the setter built on it refuses the same input rather than storing the truncated low 32 bits
    final Object previous = GlobalConfiguration.ASYNC_WORKER_THREADS.getValue();
    try {
      assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(5_000_000_000L))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger()).isNotEqualTo((int) 5_000_000_000L);
    } finally {
      GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(previous);
    }

    // a value inside the range still goes through untouched, from either form
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerce(12L)).isEqualTo(12);
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerce(12)).isEqualTo(12);
  }

  /**
   * The same bound has to hold when READING, because not every value reaches a configuration map through
   * {@code coerce}: {@link ContextConfiguration#setValue(GlobalConfiguration, Object)} is a plain map put, and
   * {@code ALTER DATABASE ... SETTING} used it directly. Widening the accessor's {@code instanceof} from
   * {@code Integer} to {@code Number} would otherwise have turned the {@code Integer.parseInt} that used to throw
   * on such a value into a silent truncation to its low 32 bits.
   */
  @Test
  void bothAccessorsRefuseAStoredIntegralValueOutsideTheIntegerRange() {
    final ContextConfiguration ctx = new ContextConfiguration();
    ctx.setValue(GlobalConfiguration.ASYNC_WORKER_THREADS, 5_000_000_000L);
    assertThatThrownBy(() -> ctx.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.asyncWorkerThreads");

    // the same text stored as a string is refused identically, so the two accessors still agree
    final ContextConfiguration textCtx = new ContextConfiguration();
    textCtx.setValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey(), "5000000000");
    assertThatThrownBy(() -> textCtx.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
        .isInstanceOf(IllegalArgumentException.class);

    // a Long inside the range is still read, not refused
    final ContextConfiguration okCtx = new ContextConfiguration();
    okCtx.setValue(GlobalConfiguration.ASYNC_WORKER_THREADS, 12L);
    assertThat(okCtx.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(12);
  }

  /**
   * A fraction is refused rather than truncated for an integral setting, from either route in: the
   * {@code Integer.parseInt} this replaced threw on {@code "6.7"}, and {@code ALTER DATABASE ... SETTING} hands
   * over whatever an arbitrary SQL expression evaluated to, so an unquoted {@code 6.7} arrives already boxed.
   */
  @Test
  void coerceRefusesAFractionalValueForAnIntegralSetting() {
    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerce(6.7d))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.asyncWorkerThreads");
    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerce(6.7f))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerce("6.7"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce(6.7d))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.commitLockTimeout");
    assertThatThrownBy(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("6.7"))
        .isInstanceOf(IllegalArgumentException.class);

    // a decimal mantissa is refused with a size suffix too, whether or not the product lands on a whole number:
    // 6.7KB is 6860.8 bytes, and 1.5MB is whole only because 0.5 is a power of two. A rule that turns on which
    // fraction happens to be binary-exact is not one an operator can predict, so neither is accepted.
    assertThatThrownBy(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("6.7KB"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("arcadedb.commitLockTimeout");
    assertThatThrownBy(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("1.5MB"))
        .isInstanceOf(IllegalArgumentException.class);

    // a Double that IS a whole number is fine, and so is a size suffix on a whole mantissa
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerce(6.0d)).isEqualTo(6);
    assertThat(GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("2MB")).isEqualTo(2L * 1024 * 1024);

    // a Float setting is unaffected: a fraction is exactly what it holds
    assertThat(GlobalConfiguration.SERVER_METRICS_TRACING_SAMPLING_RATE.coerce(0.25d)).isEqualTo(0.25f);
  }

  /** {@code coerce} returns the DECLARED type, not merely something numeric: a Long setting must not yield an Integer. */
  @Test
  void coerceReturnsTheDeclaredType() {
    assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerce("3")).isInstanceOf(Integer.class);
    assertThat(GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("3")).isInstanceOf(Long.class);
    assertThat(GlobalConfiguration.SERVER_METRICS_TRACING_SAMPLING_RATE.coerce("0.5")).isInstanceOf(Float.class);
    assertThat(GlobalConfiguration.DATE_TIME_FORMAT.coerce("yyyy")).isInstanceOf(String.class);
  }

  /**
   * The global setter and the coercion helper are the same parse by construction, so a value the helper
   * accepts is a value {@code setValue} stores, and one it refuses is one {@code setValue} refuses.
   */
  @Test
  void theGlobalSetterAndTheCoercionHelperAgree() {
    final Object previous = GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getValue();
    try {
      assertThatCode(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.setValue("1KB")).doesNotThrowAnyException();
      assertThat((Object) GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getValue()).isEqualTo(1024L);

      assertThatThrownBy(() -> GlobalConfiguration.COMMIT_LOCK_TIMEOUT.setValue("soon"))
          .isInstanceOf(IllegalArgumentException.class);
      // a rejected set leaves the previous value in place
      assertThat((Object) GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getValue()).isEqualTo(1024L);
    } finally {
      GlobalConfiguration.COMMIT_LOCK_TIMEOUT.setValue(previous);
    }
  }
}
