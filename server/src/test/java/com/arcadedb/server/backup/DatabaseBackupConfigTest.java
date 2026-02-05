/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.backup;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for DatabaseBackupConfig and its nested configuration classes.
 */
class DatabaseBackupConfigTest {

  // ============ DatabaseBackupConfig tests ============

  @Test
  void createWithDatabaseName() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");

    assertThat(config.getDatabaseName()).isEqualTo("testdb");
    assertThat(config.isEnabled()).isTrue(); // Default
    assertThat(config.getRunOnServer()).isEqualTo("$leader"); // Default
  }

  @Test
  void fromJsonBasic() {
    final JSONObject json = new JSONObject()
        .put("enabled", false)
        .put("runOnServer", "server1");

    final DatabaseBackupConfig config = DatabaseBackupConfig.fromJSON("mydb", json);

    assertThat(config.getDatabaseName()).isEqualTo("mydb");
    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getRunOnServer()).isEqualTo("server1");
  }

  @Test
  void fromJsonWithSchedule() {
    final JSONObject scheduleJson = new JSONObject()
        .put("type", "FREQUENCY")
        .put("frequencyMinutes", 30);

    final JSONObject json = new JSONObject()
        .put("schedule", scheduleJson);

    final DatabaseBackupConfig config = DatabaseBackupConfig.fromJSON("mydb", json);

    assertThat(config.getSchedule()).isNotNull();
    assertThat(config.getSchedule().getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    assertThat(config.getSchedule().getFrequencyMinutes()).isEqualTo(30);
  }

  @Test
  void fromJsonWithRetention() {
    final JSONObject retentionJson = new JSONObject()
        .put("maxFiles", 20);

    final JSONObject json = new JSONObject()
        .put("retention", retentionJson);

    final DatabaseBackupConfig config = DatabaseBackupConfig.fromJSON("mydb", json);

    assertThat(config.getRetention()).isNotNull();
    assertThat(config.getRetention().getMaxFiles()).isEqualTo(20);
  }

  @Test
  void toJsonRoundTrip() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(false);
    config.setRunOnServer("server2");

    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(45);
    config.setSchedule(schedule);

    final JSONObject json = config.toJSON();

    assertThat(json.getBoolean("enabled")).isFalse();
    assertThat(json.getString("runOnServer")).isEqualTo("server2");
    assertThat(json.has("schedule")).isTrue();
  }

  @Test
  void mergeWithDefaultsSchedule() {
    final DatabaseBackupConfig defaults = new DatabaseBackupConfig("default");
    final DatabaseBackupConfig.ScheduleConfig defaultSchedule = new DatabaseBackupConfig.ScheduleConfig();
    defaultSchedule.setFrequencyMinutes(60);
    defaults.setSchedule(defaultSchedule);

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.mergeWithDefaults(defaults);

    assertThat(config.getSchedule()).isNotNull();
    assertThat(config.getSchedule().getFrequencyMinutes()).isEqualTo(60);
  }

  @Test
  void mergeWithDefaultsRetention() {
    final DatabaseBackupConfig defaults = new DatabaseBackupConfig("default");
    final DatabaseBackupConfig.RetentionConfig defaultRetention = new DatabaseBackupConfig.RetentionConfig();
    defaultRetention.setMaxFiles(15);
    defaults.setRetention(defaultRetention);

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.mergeWithDefaults(defaults);

    assertThat(config.getRetention()).isNotNull();
    assertThat(config.getRetention().getMaxFiles()).isEqualTo(15);
  }

  @Test
  void mergeWithNullDefaults() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.mergeWithDefaults(null);

    // Should not throw, schedule and retention remain null
    assertThat(config.getSchedule()).isNull();
    assertThat(config.getRetention()).isNull();
  }

  @Test
  void mergeScheduleWithExistingConfig() {
    // When config has schedule, merge window settings from defaults
    final DatabaseBackupConfig defaults = new DatabaseBackupConfig("default");
    final DatabaseBackupConfig.ScheduleConfig defaultSchedule = new DatabaseBackupConfig.ScheduleConfig();
    defaultSchedule.setWindowStart(LocalTime.of(2, 0));
    defaultSchedule.setWindowEnd(LocalTime.of(4, 0));
    defaults.setSchedule(defaultSchedule);

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    final DatabaseBackupConfig.ScheduleConfig configSchedule = new DatabaseBackupConfig.ScheduleConfig();
    configSchedule.setFrequencyMinutes(30);
    config.setSchedule(configSchedule);

    config.mergeWithDefaults(defaults);

    assertThat(config.getSchedule().getFrequencyMinutes()).isEqualTo(30); // Own value preserved
    assertThat(config.getSchedule().getWindowStart()).isEqualTo(LocalTime.of(2, 0)); // Merged from defaults
    assertThat(config.getSchedule().getWindowEnd()).isEqualTo(LocalTime.of(4, 0)); // Merged from defaults
  }

  @Test
  void mergeRetentionWithExistingConfig() {
    // When config has retention, merge tiered settings from defaults
    final DatabaseBackupConfig defaults = new DatabaseBackupConfig("default");
    final DatabaseBackupConfig.RetentionConfig defaultRetention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(48);
    defaultRetention.setTiered(tiered);
    defaults.setRetention(defaultRetention);

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    final DatabaseBackupConfig.RetentionConfig configRetention = new DatabaseBackupConfig.RetentionConfig();
    configRetention.setMaxFiles(25);
    config.setRetention(configRetention);

    config.mergeWithDefaults(defaults);

    assertThat(config.getRetention().getMaxFiles()).isEqualTo(25); // Own value preserved
    assertThat(config.getRetention().getTiered()).isNotNull(); // Merged from defaults
    assertThat(config.getRetention().getTiered().getHourly()).isEqualTo(48);
  }

  @Test
  void validateCallsScheduleValidate() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(0); // Invalid
    config.setSchedule(schedule);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 1 minute");
  }

  @Test
  void validateCallsRetentionValidate() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    retention.setMaxFiles(0); // Invalid
    config.setRetention(retention);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 1");
  }

  // ============ ScheduleConfig tests ============

  @Test
  void scheduleConfigFromJsonFrequency() {
    final JSONObject json = new JSONObject()
        .put("type", "frequency")
        .put("frequencyMinutes", 120);

    final DatabaseBackupConfig.ScheduleConfig config = DatabaseBackupConfig.ScheduleConfig.fromJSON(json);

    assertThat(config.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    assertThat(config.getFrequencyMinutes()).isEqualTo(120);
  }

  @Test
  void scheduleConfigFromJsonCron() {
    final JSONObject json = new JSONObject()
        .put("type", "CRON")
        .put("expression", "0 0 2 * * *");

    final DatabaseBackupConfig.ScheduleConfig config = DatabaseBackupConfig.ScheduleConfig.fromJSON(json);

    assertThat(config.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    assertThat(config.getCronExpression()).isEqualTo("0 0 2 * * *");
  }

  @Test
  void scheduleConfigWithTimeWindow() {
    final JSONObject windowJson = new JSONObject()
        .put("start", "02:00")
        .put("end", "04:00");

    final JSONObject json = new JSONObject()
        .put("timeWindow", windowJson);

    final DatabaseBackupConfig.ScheduleConfig config = DatabaseBackupConfig.ScheduleConfig.fromJSON(json);

    assertThat(config.hasTimeWindow()).isTrue();
    assertThat(config.getWindowStart()).isEqualTo(LocalTime.of(2, 0));
    assertThat(config.getWindowEnd()).isEqualTo(LocalTime.of(4, 0));
  }

  @Test
  void scheduleConfigWithPartialTimeWindow() {
    // Only start specified
    final JSONObject windowJson = new JSONObject()
        .put("start", "02:00");

    final JSONObject json = new JSONObject()
        .put("timeWindow", windowJson);

    final DatabaseBackupConfig.ScheduleConfig config = DatabaseBackupConfig.ScheduleConfig.fromJSON(json);

    assertThat(config.hasTimeWindow()).isFalse(); // Needs both start and end
    assertThat(config.getWindowStart()).isEqualTo(LocalTime.of(2, 0));
    assertThat(config.getWindowEnd()).isNull();
  }

  @Test
  void scheduleConfigValidateFrequencyTooLow() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    config.setFrequencyMinutes(0);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 1 minute");
  }

  @Test
  void scheduleConfigValidateFrequencyTooHigh() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    config.setFrequencyMinutes(600000); // More than 1 year

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot exceed 1 year");
  }

  @Test
  void scheduleConfigValidateFrequencyValid() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    config.setFrequencyMinutes(60);

    // Should not throw
    config.validate();
  }

  @Test
  void scheduleConfigValidateCronMissing() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression(null);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CRON expression is required");
  }

  @Test
  void scheduleConfigValidateCronEmpty() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression("   ");

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CRON expression is required");
  }

  @Test
  void scheduleConfigValidateCronInvalid() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression("invalid");

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid CRON expression");
  }

  @Test
  void scheduleConfigValidateCronValid() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression("0 0 2 * * *"); // Every day at 2 AM

    // Should not throw
    config.validate();
  }

  @Test
  void scheduleConfigToJson() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    config.setFrequencyMinutes(30);
    config.setWindowStart(LocalTime.of(1, 0));
    config.setWindowEnd(LocalTime.of(5, 0));

    final JSONObject json = config.toJSON();

    assertThat(json.getString("type")).isEqualTo("frequency");
    assertThat(json.getInt("frequencyMinutes")).isEqualTo(30);
    assertThat(json.has("timeWindow")).isTrue();
  }

  @Test
  void scheduleConfigToJsonCron() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression("0 0 3 * * MON");

    final JSONObject json = config.toJSON();

    assertThat(json.getString("type")).isEqualTo("cron");
    assertThat(json.getString("expression")).isEqualTo("0 0 3 * * MON");
    assertThat(json.has("frequencyMinutes")).isFalse();
  }

  @Test
  void scheduleConfigMergeTimeWindow() {
    final DatabaseBackupConfig.ScheduleConfig defaults = new DatabaseBackupConfig.ScheduleConfig();
    defaults.setWindowStart(LocalTime.of(2, 0));
    defaults.setWindowEnd(LocalTime.of(4, 0));

    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.mergeWithDefaults(defaults);

    assertThat(config.getWindowStart()).isEqualTo(LocalTime.of(2, 0));
    assertThat(config.getWindowEnd()).isEqualTo(LocalTime.of(4, 0));
  }

  @Test
  void scheduleConfigMergePreservesExisting() {
    final DatabaseBackupConfig.ScheduleConfig defaults = new DatabaseBackupConfig.ScheduleConfig();
    defaults.setWindowStart(LocalTime.of(2, 0));
    defaults.setWindowEnd(LocalTime.of(4, 0));

    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setWindowStart(LocalTime.of(1, 0)); // Own value

    config.mergeWithDefaults(defaults);

    assertThat(config.getWindowStart()).isEqualTo(LocalTime.of(1, 0)); // Preserved
    assertThat(config.getWindowEnd()).isEqualTo(LocalTime.of(4, 0)); // Merged
  }

  @Test
  void scheduleConfigDefaultType() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();

    assertThat(config.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    assertThat(config.getFrequencyMinutes()).isEqualTo(60); // Default
  }

  // ============ RetentionConfig tests ============

  @Test
  void retentionConfigFromJson() {
    final JSONObject json = new JSONObject()
        .put("maxFiles", 25);

    final DatabaseBackupConfig.RetentionConfig config = DatabaseBackupConfig.RetentionConfig.fromJSON(json);

    assertThat(config.getMaxFiles()).isEqualTo(25);
  }

  @Test
  void retentionConfigWithTiered() {
    final JSONObject tieredJson = new JSONObject()
        .put("hourly", 12)
        .put("daily", 5)
        .put("weekly", 2)
        .put("monthly", 6)
        .put("yearly", 1);

    final JSONObject json = new JSONObject()
        .put("tiered", tieredJson);

    final DatabaseBackupConfig.RetentionConfig config = DatabaseBackupConfig.RetentionConfig.fromJSON(json);

    assertThat(config.hasTieredRetention()).isTrue();
    assertThat(config.getTiered().getHourly()).isEqualTo(12);
    assertThat(config.getTiered().getDaily()).isEqualTo(5);
    assertThat(config.getTiered().getWeekly()).isEqualTo(2);
    assertThat(config.getTiered().getMonthly()).isEqualTo(6);
    assertThat(config.getTiered().getYearly()).isEqualTo(1);
  }

  @Test
  void retentionConfigValidateMaxFilesTooLow() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.setMaxFiles(0);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 1");
  }

  @Test
  void retentionConfigValidateMaxFilesTooHigh() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.setMaxFiles(20000);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot exceed 10000");
  }

  @Test
  void retentionConfigValidateMaxFilesValid() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.setMaxFiles(100);

    // Should not throw
    config.validate();
  }

  @Test
  void retentionConfigValidatesTiered() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(-1);
    config.setTiered(tiered);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void retentionConfigToJson() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.setMaxFiles(50);

    final JSONObject json = config.toJSON();

    assertThat(json.getInt("maxFiles")).isEqualTo(50);
  }

  @Test
  void retentionConfigToJsonWithTiered() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.setMaxFiles(50);
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setDaily(14);
    config.setTiered(tiered);

    final JSONObject json = config.toJSON();

    assertThat(json.getInt("maxFiles")).isEqualTo(50);
    assertThat(json.has("tiered")).isTrue();
    assertThat(json.getJSONObject("tiered").getInt("daily")).isEqualTo(14);
  }

  @Test
  void retentionConfigMergeWithDefaults() {
    final DatabaseBackupConfig.RetentionConfig defaults = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    tiered.setHourly(48);
    defaults.setTiered(tiered);

    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.mergeWithDefaults(defaults);

    assertThat(config.getTiered()).isNotNull();
    assertThat(config.getTiered().getHourly()).isEqualTo(48);
  }

  @Test
  void retentionConfigMergePreservesExisting() {
    final DatabaseBackupConfig.RetentionConfig defaults = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    defaults.setTiered(tiered);

    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig ownTiered = new DatabaseBackupConfig.TieredConfig();
    ownTiered.setHourly(100);
    config.setTiered(ownTiered);

    config.mergeWithDefaults(defaults);

    assertThat(config.getTiered().getHourly()).isEqualTo(100); // Preserved
  }

  @Test
  void retentionConfigDefaultMaxFiles() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();

    assertThat(config.getMaxFiles()).isEqualTo(10); // Default
  }

  // ============ TieredConfig tests ============

  @Test
  void tieredConfigFromJson() {
    final JSONObject json = new JSONObject()
        .put("hourly", 48)
        .put("daily", 14)
        .put("weekly", 8)
        .put("monthly", 24)
        .put("yearly", 5);

    final DatabaseBackupConfig.TieredConfig config = DatabaseBackupConfig.TieredConfig.fromJSON(json);

    assertThat(config.getHourly()).isEqualTo(48);
    assertThat(config.getDaily()).isEqualTo(14);
    assertThat(config.getWeekly()).isEqualTo(8);
    assertThat(config.getMonthly()).isEqualTo(24);
    assertThat(config.getYearly()).isEqualTo(5);
  }

  @Test
  void tieredConfigDefaults() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();

    assertThat(config.getHourly()).isEqualTo(24);
    assertThat(config.getDaily()).isEqualTo(7);
    assertThat(config.getWeekly()).isEqualTo(4);
    assertThat(config.getMonthly()).isEqualTo(12);
    assertThat(config.getYearly()).isEqualTo(3);
  }

  @Test
  void tieredConfigValidateNegativeHourly() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setHourly(-1);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void tieredConfigValidateNegativeDaily() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setDaily(-1);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void tieredConfigValidateNegativeWeekly() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setWeekly(-1);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void tieredConfigValidateNegativeMonthly() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setMonthly(-1);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void tieredConfigValidateNegativeYearly() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setYearly(-1);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void tieredConfigValidateTooHighHourly() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setHourly(2000);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot exceed 1000");
  }

  @Test
  void tieredConfigValidateTooHighDaily() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setDaily(2000);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot exceed 1000");
  }

  @Test
  void tieredConfigValidateValid() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();

    // Should not throw with defaults
    config.validate();
  }

  @Test
  void tieredConfigToJson() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setHourly(10);
    config.setDaily(5);

    final JSONObject json = config.toJSON();

    assertThat(json.getInt("hourly")).isEqualTo(10);
    assertThat(json.getInt("daily")).isEqualTo(5);
    assertThat(json.getInt("weekly")).isEqualTo(4); // Default
    assertThat(json.getInt("monthly")).isEqualTo(12); // Default
    assertThat(json.getInt("yearly")).isEqualTo(3); // Default
  }

  // ============ Setters tests ============

  @Test
  void settersWork() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("db");
    config.setEnabled(false);
    config.setRunOnServer("myserver");

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getRunOnServer()).isEqualTo("myserver");
  }

  @Test
  void scheduleSettersWork() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression("0 0 * * * *");
    config.setFrequencyMinutes(99);

    assertThat(config.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    assertThat(config.getCronExpression()).isEqualTo("0 0 * * * *");
    assertThat(config.getFrequencyMinutes()).isEqualTo(99);
  }

  @Test
  void retentionSettersWork() {
    final DatabaseBackupConfig.RetentionConfig retention = new DatabaseBackupConfig.RetentionConfig();
    final DatabaseBackupConfig.TieredConfig tiered = new DatabaseBackupConfig.TieredConfig();
    retention.setTiered(tiered);

    assertThat(retention.getTiered()).isSameAs(tiered);
  }

  @Test
  void tieredSettersWork() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setWeekly(10);
    config.setMonthly(20);
    config.setYearly(30);

    assertThat(config.getWeekly()).isEqualTo(10);
    assertThat(config.getMonthly()).isEqualTo(20);
    assertThat(config.getYearly()).isEqualTo(30);
  }

  // ============ Additional edge cases ============

  @Test
  void fromJsonWithEmptyObject() {
    final JSONObject json = new JSONObject();

    final DatabaseBackupConfig config = DatabaseBackupConfig.fromJSON("mydb", json);

    assertThat(config.getDatabaseName()).isEqualTo("mydb");
    assertThat(config.isEnabled()).isTrue(); // Default
    assertThat(config.getRunOnServer()).isEqualTo("$leader"); // Default
    assertThat(config.getSchedule()).isNull();
    assertThat(config.getRetention()).isNull();
  }

  @Test
  void scheduleConfigFromJsonWithDefaults() {
    final JSONObject json = new JSONObject(); // Empty

    final DatabaseBackupConfig.ScheduleConfig config = DatabaseBackupConfig.ScheduleConfig.fromJSON(json);

    assertThat(config.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    assertThat(config.getFrequencyMinutes()).isEqualTo(60);
  }

  @Test
  void hasTieredRetentionFalse() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();

    assertThat(config.hasTieredRetention()).isFalse();
  }

  @Test
  void hasTimeWindowFalse() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();

    assertThat(config.hasTimeWindow()).isFalse();
  }
}
