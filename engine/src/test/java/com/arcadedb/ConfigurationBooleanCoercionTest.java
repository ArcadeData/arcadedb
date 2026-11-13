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
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigurationBooleanCoercionTest {
  /**
   * Regression for #6875: two defects that compound in the setting value coercion of the administrative writers.
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
  @Nested
  class SettingValueCoercion {

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

  /**
   * Regression for #7124: a {@code Boolean} setting was the one type an administrative command could get wrong for free.
   * {@code GlobalConfiguration.coerce} answers {@code false} for anything it cannot read - deliberately, because it
   * runs inside that class's static initializer - so {@code SET SERVER SETTING ... requireAuthentication ture} was
   * stored as {@code Boolean.FALSE} and answered with a 200, and the text that produced it was gone before any reader
   * could tell the typo from a deliberate {@code false}.
   * <p>
   * {@link GlobalConfiguration#coerceFromAdminCommand(Object)} is the strict entry point the administrative paths use.
   * Its behaviour has to differ from {@code coerce} for exactly one type and be identical for every other.
   */
  @Nested
  class BooleanSettingStrictCoercion {

    private static final GlobalConfiguration BOOLEAN_SETTING = GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;

    @Test
    void aBooleanTypoIsRefusedRatherThanReadAsFalse() {
      // WHAT THE PERMISSIVE PATH DOES, AND MUST KEEP DOING: THE STATIC INITIALIZER CANNOT THROW.
      assertThat(BOOLEAN_SETTING.coerce("ture")).isEqualTo(Boolean.FALSE);

      assertThatThrownBy(() -> BOOLEAN_SETTING.coerceFromAdminCommand("ture")).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(BOOLEAN_SETTING.getKey()).hasMessageContaining("ture");
    }

    @Test
    void everyOtherNonBooleanTextIsRefusedToo() {
      for (final String notABoolean : new String[] { "yes", "no", "1", "0", "on", "off", "" })
        assertThatThrownBy(() -> BOOLEAN_SETTING.coerceFromAdminCommand(notABoolean)).as(
            "'%s' is not a boolean and must not be folded to false", notABoolean).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bothBooleanLiteralsAreAcceptedCaseInsensitivelyAndTrimmed() {
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand("true")).isEqualTo(Boolean.TRUE);
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand("false")).isEqualTo(Boolean.FALSE);
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(" TRUE ")).isEqualTo(Boolean.TRUE);
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand("False")).isEqualTo(Boolean.FALSE);
    }

    @Test
    void anActualBooleanPassesThrough() {
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(Boolean.TRUE)).isEqualTo(Boolean.TRUE);
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(Boolean.FALSE)).isEqualTo(Boolean.FALSE);
      assertThat(BOOLEAN_SETTING.coerceFromAdminCommand(null)).isNull();
    }

    @Test
    void everyOtherSettingTypeBehavesExactlyAsCoerceDoes() {
      // THE STRICT PATH ADDS A RULE FOR ONE TYPE AND MUST CHANGE NOTHING ELSE, INCLUDING WHAT IT ALREADY REFUSES.
      assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerceFromAdminCommand("12")).isEqualTo(12);
      assertThat(GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerceFromAdminCommand("64GB")).isEqualTo(
          GlobalConfiguration.COMMIT_LOCK_TIMEOUT.coerce("64GB"));
      assertThat(GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT.coerceFromAdminCommand("ture")).isEqualTo("ture");

      assertThatThrownBy(() -> GlobalConfiguration.ASYNC_WORKER_THREADS.coerceFromAdminCommand("abc")).isInstanceOf(
          NumberFormatException.class);
    }

    @Test
    void everyBooleanSettingGetsTheGuardNotJustThisOne() {
      // THE RULE IS THE TYPE'S, NOT ONE SETTING'S: A LATER Boolean SETTING INHERITS IT WITHOUT ANYONE REMEMBERING TO.
      int booleanSettings = 0;
      for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
        if (setting.getType() != Boolean.class)
          continue;

        booleanSettings++;
        assertThatThrownBy(() -> setting.coerceFromAdminCommand("ture")).as("setting '%s'", setting.getKey())
            .isInstanceOf(IllegalArgumentException.class);
      }

      assertThat(booleanSettings).isGreaterThan(1);
    }
  }

  /**
   * Regression for #7222: the Prometheus requireAuthentication setting kept its STRICT boolean parse when read from system properties and environment variables.
   * <p>
   * {@code arcadedb.serverMetrics.prometheus.requireAuthentication} is documented as having a STRICT parse so that "a
   * typo cannot silently publish the endpoint unauthenticated". That held on the two administrative writers and not on
   * the third one - {@link GlobalConfiguration#readConfiguration()}, which feeds every system property and environment
   * variable through the PERMISSIVE {@code coerce}, where {@code Boolean.parseBoolean} maps everything that is not
   * {@code "true"} to {@code false} with no error.
   * <p>
   * So {@code requireAuthentication=yes} in a Kubernetes env var stored {@code Boolean.FALSE}, the plugin's own strict
   * check found a value that was already a {@code Boolean} and had nothing left to reject, and {@code /prometheus} was
   * published unauthenticated while the operator believed they had turned the protection ON. The refusal has to happen
   * where the text still exists.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StrictBooleanFromConfigurationSource {

    private static final GlobalConfiguration BOOLEAN_SETTING_DEFAULTING_TO_TRUE  =
        GlobalConfiguration.SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION;
    private static final GlobalConfiguration BOOLEAN_SETTING_DEFAULTING_TO_FALSE = GlobalConfiguration.NETWORK_USE_SSL;

    @AfterEach
    void restoreTheSettings() {
      BOOLEAN_SETTING_DEFAULTING_TO_TRUE.reset();
      BOOLEAN_SETTING_DEFAULTING_TO_FALSE.reset();
      GlobalConfiguration.SERVER_DATABASE_DIRECTORY.reset();
    }

    @Test
    void aValueThatIsNeitherTrueNorFalseIsRefusedRatherThanReadAsFalse() {
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getDefValue()).isEqualTo(Boolean.TRUE);

      for (final String typo : new String[] { "yes", "1", "on", "TRUE!", "ture", "" }) {
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(typo, "system property")).as(
            "'%s' was accepted", typo).isFalse();
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).as("'%s' turned the setting OFF", typo).isTrue();
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.isChanged()).as("'%s' was recorded as an explicit choice", typo)
            .isFalse();
      }
    }

    /**
     * The refusal keeps the DEFAULT rather than forcing {@code true}: what makes this setting fail closed is that its
     * own default is the safe value, and a general rule that read every unparseable boolean as {@code true} would flip
     * settings whose safe side is the other one. Refusing to guess is the property that has to hold for all of them.
     */
    @Test
    void aRefusedValueKeepsTheSettingsOwnDefaultInBothDirections() {
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_FALSE.getDefValue()).isEqualTo(Boolean.FALSE);

      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_FALSE.setValueFromConfigurationSource("yes", "environment variable"))
          .isFalse();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_FALSE.getValueAsBoolean()).isFalse();
    }

    @Test
    void trueAndFalseAreStillHonouredInEveryCasingAndWithSurroundingSpace() {
      for (final String text : new String[] { "false", "FALSE", "  False  " }) {
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(text, "system property")).isTrue();
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).as("'%s'", text).isFalse();
      }

      for (final String text : new String[] { "true", "TRUE", " True " }) {
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(text, "system property")).isTrue();
        assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).as("'%s'", text).isTrue();
      }
    }

    /**
     * Only {@code Boolean} settings gain the strict parse. Every other type already refuses what it cannot read, and
     * the leniency this path needs is about not taking the engine down over one bad value - not about accepting one.
     */
    @Test
    void aNonBooleanSettingIsUnaffected() {
      assertThat(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.setValueFromConfigurationSource("/mnt/data", "system property"))
          .isTrue();
      assertThat(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString()).isEqualTo("/mnt/data");
    }

    /**
     * An unreadable value of a non-Boolean setting must not take the whole engine down: this method runs inside this
     * class's static initializer, where a throw becomes an {@code ExceptionInInitializerError}. It is reported and the
     * setting keeps its default, which is the same answer the Boolean arm gives.
     */
    @Test
    void anUnreadableValueOfAnyTypeIsReportedRatherThanThrown() {
      final int previous = GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger();

      assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.setValueFromConfigurationSource("abc", "environment variable"))
          .isFalse();
      assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger()).isEqualTo(previous);
    }

    /**
     * The refusal REPORTS the value it refused, which is what makes the warning actionable - and the value is text an
     * operator typed, so a hidden setting must not put it in a log. No hidden setting is of a type that can fail to
     * coerce today (they are all {@code String}), which is exactly why the rule has to be pinned now rather than
     * discovered by the first one that is not.
     */
    @Test
    void aHiddenSettingsValueIsNotWrittenToTheLog() {
      assertThat(GlobalConfiguration.SERVER_ROOT_PASSWORD.isHidden()).isTrue();
      assertThat(GlobalConfiguration.SERVER_ROOT_PASSWORD.redactIfHidden("hunter2")).isEqualTo("<hidden>");

      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.isHidden()).isFalse();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.redactIfHidden("yes")).isEqualTo("'yes'");
    }

    /**
     * "Keeps the default" has to mean the default the setting would have had, and for a setting that AUTO-SIZES one
     * that is not its compiled-in {@code defValue}. {@code QUERY_MAX_RANGE_SIZE} scales its cap with the JVM heap
     * through {@code callbackIfNoSet}, and its {@code defValue} is the unbounded-heap figure - so a refused value
     * that fell through to {@code defValue} would RAISE the cap on a small-heap JVM, which for a setting whose whole
     * job is to bound the memory one query can ask for is the wrong direction to move on a typo.
     */
    @Test
    void aRefusedValueTakesTheSameBranchAsAnAbsentOne() {
      // SERVER_HTTP_IO_THREADS rather than QUERY_MAX_RANGE_SIZE: its auto-sized value is availableProcessors(), which
      // is never its defValue of 0 on any machine. Sizing QUERY_MAX_RANGE_SIZE off the heap would make this test
      // silently vacuous on a JVM whose heap happens to land on the compiled-in figure.
      final GlobalConfiguration autoSized = GlobalConfiguration.SERVER_HTTP_IO_THREADS;
      try {
        autoSized.applyConfigurationSource(null, "system property");
        final int whenAbsent = autoSized.getValueAsInteger();

        // The value the auto-sizing exists to avoid, so a refusal falling through to it is visible here.
        assertThat(whenAbsent).isNotEqualTo(autoSized.getDefValue());

        autoSized.applyConfigurationSource("not-a-number", "environment variable");

        assertThat(autoSized.getValueAsInteger()).isEqualTo(whenAbsent);
      } finally {
        autoSized.reset();
      }
    }

    /**
     * The "nothing was chosen" half of a refusal has to survive a throw from INSIDE {@link GlobalConfiguration#setValue}
     * too, not only from the strict parse that runs before it. {@code setValue} marked the setting explicitly-set
     * before doing the work and, on failure, rolled back the value but not that flag - so a rejected write left
     * {@code isChanged()} reporting a setting as configured while it sat at its default. The strict parse catches a bad
     * Boolean first, so no setting here could reach it; a callback or an {@code allowed} set that throws can.
     */
    @Test
    void aWriteThatWasRolledBackIsNotRecordedAsAChoice() {
      final GlobalConfiguration withAllowedValues = GlobalConfiguration.BUCKET_REUSE_SPACE_MODE;
      try {
        assertThat(withAllowedValues.isChanged()).isFalse();

        assertThatThrownBy(() -> withAllowedValues.setValue("not-an-allowed-value")).isInstanceOf(
            IllegalArgumentException.class);

        assertThat(withAllowedValues.isChanged()).as("a refused write was recorded as an explicit choice").isFalse();
        assertThat(withAllowedValues.getValueAsString()).isEqualTo(withAllowedValues.getDefValue());
      } finally {
        withAllowedValues.reset();
      }
    }

    /** A value that arrives already typed - a callback's return, a programmatic write - is not text and is stored. */
    @Test
    void anAlreadyTypedValueIsStored() {
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.setValueFromConfigurationSource(Boolean.FALSE, "system property"))
          .isTrue();
      assertThat(BOOLEAN_SETTING_DEFAULTING_TO_TRUE.getValueAsBoolean()).isFalse();
    }
  }

  /**
   * Regression for #7262: the server configuration file used to store its values into the {@link ContextConfiguration} overlay
   * with no coercion, and {@link ContextConfiguration#getValueAsBoolean(GlobalConfiguration)} used to read them back
   * with {@code Boolean.parseBoolean}, which maps every string that is not {@code "true"} to {@code false}.
   * <p>
   * Composed, that turned {@code "arcadedb.ha.tls.mutualAuth": "yes"} into a silent shutdown of mutual TLS
   * authentication on the inter-node Raft channel, and {@code "arcadedb.ha.peerAllowlist.enabled": "on"} into a
   * server that never installs the peer allowlist. Both settings default to {@code true}, so the operator who left
   * them alone was protected and the operator who wrote down that they wanted the protection was not.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConfigurationFileBooleanStrict {

    private static final GlobalConfiguration MUTUAL_AUTH      = GlobalConfiguration.HA_TLS_MUTUAL_AUTH;
    private static final GlobalConfiguration PEER_ALLOWLIST   = GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED;

    @Test
    public void bothSecuritySwitchesDefaultToTrue() {
      // THE WHOLE POINT OF THE ISSUE: THE DEFAULT IS THE PROTECTION, SO A MISREAD CAN ONLY LOSE IT.
      assertThat(MUTUAL_AUTH.getDefValue()).isEqualTo(Boolean.TRUE);
      assertThat(PEER_ALLOWLIST.getDefValue()).isEqualTo(Boolean.TRUE);
    }

    @Test
    public void aSynonymInTheConfigurationFileNoLongerDisablesMutualTLS() {
      for (final String synonym : new String[] { "yes", "on", "Y", "1", "ture", "true!" }) {
        final ContextConfiguration cfg = new ContextConfiguration();
        cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"" + synonym + "\""));

        // Against the setting's own value, not a hard-coded true: what a refusal keeps is "whatever it would have
        // been without this line", which a -D in the surrounding run is entitled to have chosen.
        assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).as("mutual TLS auth with '%s' in the configuration file", synonym)
            .isEqualTo(MUTUAL_AUTH.getValueAsBoolean());
      }
    }

    @Test
    public void surroundingWhitespaceIsAcceptedRatherThanRefused() {
      // Trimmed before the comparison, so this is a VALID true - it is not one of the refused spellings above.
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\" TRUE \""));

      assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).isEqualTo(Boolean.TRUE);
      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isTrue();
    }

    @Test
    public void aSynonymInTheConfigurationFileNoLongerSkipsThePeerAllowlist() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(PEER_ALLOWLIST, "\"on\""));

      assertThat(cfg.getValueAsBoolean(PEER_ALLOWLIST)).isEqualTo(PEER_ALLOWLIST.getValueAsBoolean());
    }

    @Test
    public void aRefusedValueIsNotStoredAtAll() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"yes\""));

      // NOT MERELY READ AS THE DEFAULT: THE OVERLAY IS WHAT A SERVER HANDS TO ITS PLUGINS, AND A VALUE NOTHING CAN
      // READ HAS NO BUSINESS BEING IN IT.
      assertThat(cfg.hasValue(MUTUAL_AUTH.getKey())).isFalse();
    }

    @Test
    public void anExplicitFalseIsStillHonoured() {
      for (final String text : new String[] { "false", "False", "FALSE", " false " }) {
        final ContextConfiguration cfg = new ContextConfiguration();
        cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"" + text + "\""));

        assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).as("explicit '%s'", text).isFalse();
        assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).as("stored as the declared type").isEqualTo(Boolean.FALSE);
      }
    }

    @Test
    public void anExplicitTrueIsStillHonoured() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"true\""));

      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isTrue();
      assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).isEqualTo(Boolean.TRUE);
    }

    @Test
    public void aGenuineJSONBooleanStillWorks() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(MUTUAL_AUTH, "false"));

      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isFalse();
    }

    @Test
    public void theConfigurationFileStoresValuesAsTheirDeclaredType() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON("{\"configuration\":{\"" + shortKey(GlobalConfiguration.COMMIT_LOCK_TIMEOUT) + "\":\"64\"}}");

      assertThat(cfg.<Object>getValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT)).isInstanceOf(Long.class);
      assertThat(cfg.getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT)).isEqualTo(64L);
    }

    @Test
    public void anUnreadableNonBooleanIsRefusedToo() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON("{\"configuration\":{\"" + shortKey(GlobalConfiguration.ASYNC_WORKER_THREADS) + "\":\"abc\"}}");

      assertThat(cfg.hasValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey())).isFalse();
      assertThat(cfg.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(
          GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger());
    }

    /**
     * The reader is the second half of the mechanism, and it has its own way in: the map constructor stores whatever
     * it is handed without ever passing through {@link ContextConfiguration#fromJSON(String)}.
     */
    @Test
    public void theReaderFallsBackToTheDefaultRatherThanToFalse() {
      final Map<String, Object> raw = new HashMap<>();
      raw.put(MUTUAL_AUTH.getKey(), "yes");
      raw.put(GlobalConfiguration.HA_ENABLED.getKey(), "on");

      final ContextConfiguration cfg = new ContextConfiguration(raw);

      // DEFAULT true: THE REFUSED VALUE MUST NOT TURN THE PROTECTION OFF.
      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isTrue();
      // DEFAULT false: REFUSING TO GUESS IS THE PROPERTY THAT HOLDS FOR ALL OF THEM, NOT "ALWAYS true".
      assertThat(GlobalConfiguration.HA_ENABLED.getDefValue()).isEqualTo(Boolean.FALSE);
      assertThat(cfg.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isFalse();
    }

    @Test
    public void theReaderStillReadsBooleanTextInEitherCaseAndWithSpaces() {
      final Map<String, Object> raw = new HashMap<>();
      raw.put(GlobalConfiguration.HA_ENABLED.getKey(), " TRUE ");
      assertThat(new ContextConfiguration(raw).getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isTrue();

      raw.put(GlobalConfiguration.HA_ENABLED.getKey(), "False");
      assertThat(new ContextConfiguration(raw).getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isFalse();
    }

    @Test
    public void anUnknownSettingInTheConfigurationFileIsStillIgnored() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON("{\"configuration\":{\"not.a.real.setting\":\"yes\"}}");

      assertThat(cfg.getContextKeys()).isEmpty();
    }

    /**
     * The allow-list is enforced at this entry point too, and it has to be: {@code coerce} converts to the declared
     * TYPE and stops, so a {@code String} setting with an allow-list used to accept anything that was a string. A
     * server configuration file naming a mode that does not exist was stored verbatim, every reader compared it
     * against {@code "production"}, found it different, and gave a production deployment the development behaviour -
     * Studio included.
     */
    @Test
    public void aValueOutsideTheAllowListIsRefusedByTheConfigurationFile() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(GlobalConfiguration.SERVER_MODE, "\"staging\""));

      assertThat(cfg.hasValue(GlobalConfiguration.SERVER_MODE.getKey())).isFalse();
      assertThat(cfg.getValueAsString(GlobalConfiguration.SERVER_MODE)).isEqualTo(
          GlobalConfiguration.SERVER_MODE.getValueAsString());
    }

    /**
     * The allow-list is matched case-insensitively, so {@code Production} was accepted and then stored with the
     * capital - which no {@code "production".equals(mode)} reader recognises. Normalised to the declared spelling in
     * the one conversion both writers go through, so every reader sees it.
     */
    @Test
    public void anAllowListedValueIsNormalisedToItsDeclaredSpelling() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(GlobalConfiguration.SERVER_MODE, "\"Production\""));

      assertThat(cfg.getValueAsString(GlobalConfiguration.SERVER_MODE)).isEqualTo("production");
      assertThat(GlobalConfiguration.SERVER_MODE.coerceFromAdminCommand("PRODUCTION")).isEqualTo("production");
    }

    /**
     * The property path may not throw: it runs from {@code readConfiguration()} inside {@code GlobalConfiguration}'s
     * static initializer, where an escaping exception becomes an {@code ExceptionInInitializerError} that takes the
     * engine down over one mistyped variable. {@code setValue} refuses an allow-list violation by THROWING, so it
     * has to stay inside the non-throwing envelope.
     */
    @Test
    public void aValueOutsideTheAllowListIsReportedRatherThanThrownOnThePropertyPath() {
      final String before = GlobalConfiguration.SERVER_MODE.getValueAsString();
      final boolean wasChanged = GlobalConfiguration.SERVER_MODE.isChanged();
      try {
        assertThat(GlobalConfiguration.SERVER_MODE.setValueFromConfigurationSource("staging", "system property"))
            .isFalse();

        // A refusal is not a choice anyone made, so it must leave BOTH halves of the setting's state alone.
        assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo(before);
        assertThat(GlobalConfiguration.SERVER_MODE.isChanged()).isEqualTo(wasChanged);
      } finally {
        restore(GlobalConfiguration.SERVER_MODE, before, wasChanged);
      }
    }

    @Test
    public void anAllowListedValueStillGoesThroughOnThePropertyPath() {
      final String before = GlobalConfiguration.SERVER_MODE.getValueAsString();
      final boolean wasChanged = GlobalConfiguration.SERVER_MODE.isChanged();
      try {
        assertThat(GlobalConfiguration.SERVER_MODE.setValueFromConfigurationSource("Test", "system property")).isTrue();
        assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("test");
      } finally {
        restore(GlobalConfiguration.SERVER_MODE, before, wasChanged);
      }
    }

    /**
     * Puts a process-wide setting back exactly as it was, {@link GlobalConfiguration#isChanged()} included.
     * {@code reset()} alone is not that: it restores the COMPILED-IN default, so a test that borrows a setting
     * would silently discard a {@code -D} the surrounding run was given and leave every later test reading the
     * wrong value.
     */
    private static void restore(final GlobalConfiguration setting, final Object value, final boolean wasChanged) {
      if (wasChanged)
        setting.setValue(value);
      else
        setting.reset();
    }

    private static String configurationFile(final GlobalConfiguration setting, final String jsonValue) {
      return "{\"configuration\":{\"" + shortKey(setting) + "\":" + jsonValue + "}}";
    }

    private static String shortKey(final GlobalConfiguration setting) {
      return setting.getKey().substring(GlobalConfiguration.PREFIX.length());
    }
  }
}
