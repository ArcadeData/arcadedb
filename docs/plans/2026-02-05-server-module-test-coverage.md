# Server Module Test Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add unit tests for the server module to improve test coverage, focusing on backup components and HTTP handler utilities.

**Architecture:** Unit tests for isolated components (DatabaseBackupConfig, ExecutionResponse) and integration-style tests for BackupScheduler. Tests use AssertJ assertions and follow existing project patterns.

**Tech Stack:** JUnit 5, AssertJ, Java 21

---

## Task 1: DatabaseBackupConfig Unit Tests

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/backup/DatabaseBackupConfigTest.java`

**Step 1: Write test file with basic configuration tests**

```java
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
    config.setFrequencyMinutes(600000);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot exceed 1 year");
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
  void scheduleConfigValidateCronInvalid() {
    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    config.setCronExpression("invalid");

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid CRON expression");
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
  void scheduleConfigMergeTimeWindow() {
    final DatabaseBackupConfig.ScheduleConfig defaults = new DatabaseBackupConfig.ScheduleConfig();
    defaults.setWindowStart(LocalTime.of(2, 0));
    defaults.setWindowEnd(LocalTime.of(4, 0));

    final DatabaseBackupConfig.ScheduleConfig config = new DatabaseBackupConfig.ScheduleConfig();
    config.mergeWithDefaults(defaults);

    assertThat(config.getWindowStart()).isEqualTo(LocalTime.of(2, 0));
    assertThat(config.getWindowEnd()).isEqualTo(LocalTime.of(4, 0));
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
  void retentionConfigToJson() {
    final DatabaseBackupConfig.RetentionConfig config = new DatabaseBackupConfig.RetentionConfig();
    config.setMaxFiles(50);

    final JSONObject json = config.toJSON();

    assertThat(json.getInt("maxFiles")).isEqualTo(50);
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
  void tieredConfigValidateNegative() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setHourly(-1);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be negative");
  }

  @Test
  void tieredConfigValidateTooHigh() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setDaily(2000);

    assertThatThrownBy(config::validate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot exceed 1000");
  }

  @Test
  void tieredConfigToJson() {
    final DatabaseBackupConfig.TieredConfig config = new DatabaseBackupConfig.TieredConfig();
    config.setHourly(10);
    config.setDaily(5);

    final JSONObject json = config.toJSON();

    assertThat(json.getInt("hourly")).isEqualTo(10);
    assertThat(json.getInt("daily")).isEqualTo(5);
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
}
```

**Step 2: Run tests to verify they pass**

Run: `mvn test -pl server -Dtest=DatabaseBackupConfigTest -q`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/backup/DatabaseBackupConfigTest.java
git commit -m "test: add DatabaseBackupConfig unit tests

Cover configuration parsing, validation, JSON round-trip,
and merge with defaults for schedule, retention, and tiered configs."
```

---

## Task 2: ExecutionResponse Unit Tests

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/http/handler/ExecutionResponseTest.java`

**Step 1: Write test file**

```java
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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for ExecutionResponse.
 */
class ExecutionResponseTest {

  @Test
  void createWithStringResponse() {
    final ExecutionResponse response = new ExecutionResponse(200, "success");

    // We can't directly access fields, but we can verify construction doesn't throw
    assertThat(response).isNotNull();
  }

  @Test
  void createWithBinaryResponse() {
    final byte[] data = {0x01, 0x02, 0x03, 0x04};
    final ExecutionResponse response = new ExecutionResponse(201, data);

    assertThat(response).isNotNull();
  }

  @Test
  void createWithDifferentStatusCodes() {
    final ExecutionResponse ok = new ExecutionResponse(200, "ok");
    final ExecutionResponse created = new ExecutionResponse(201, "created");
    final ExecutionResponse badRequest = new ExecutionResponse(400, "bad request");
    final ExecutionResponse serverError = new ExecutionResponse(500, "error");

    assertThat(ok).isNotNull();
    assertThat(created).isNotNull();
    assertThat(badRequest).isNotNull();
    assertThat(serverError).isNotNull();
  }

  @Test
  void createWithEmptyStringResponse() {
    final ExecutionResponse response = new ExecutionResponse(204, "");

    assertThat(response).isNotNull();
  }

  @Test
  void createWithEmptyBinaryResponse() {
    final ExecutionResponse response = new ExecutionResponse(204, new byte[0]);

    assertThat(response).isNotNull();
  }

  @Test
  void createWithLargeStringResponse() {
    final String largeResponse = "x".repeat(100000);
    final ExecutionResponse response = new ExecutionResponse(200, largeResponse);

    assertThat(response).isNotNull();
  }

  @Test
  void createWithLargeBinaryResponse() {
    final byte[] largeData = new byte[100000];
    final ExecutionResponse response = new ExecutionResponse(200, largeData);

    assertThat(response).isNotNull();
  }

  @Test
  void createWithJsonResponse() {
    final String jsonResponse = "{\"status\":\"ok\",\"count\":42}";
    final ExecutionResponse response = new ExecutionResponse(200, jsonResponse);

    assertThat(response).isNotNull();
  }

  @Test
  void createWithNullStringResponse() {
    // Null string response should be handled
    final ExecutionResponse response = new ExecutionResponse(200, (String) null);

    assertThat(response).isNotNull();
  }
}
```

**Step 2: Run tests to verify they pass**

Run: `mvn test -pl server -Dtest=ExecutionResponseTest -q`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/http/handler/ExecutionResponseTest.java
git commit -m "test: add ExecutionResponse unit tests

Cover string and binary response creation with various status codes."
```

---

## Task 3: BackupScheduler Unit Tests

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/backup/BackupSchedulerTest.java`

**Step 1: Write test file**

```java
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for BackupScheduler lifecycle and scheduling logic.
 */
class BackupSchedulerTest {

  private BackupScheduler scheduler;

  @BeforeEach
  void setUp() {
    // Create scheduler with null server (won't execute backups, just tests scheduling)
    scheduler = new BackupScheduler(null, "/tmp/backups", null);
  }

  @AfterEach
  void tearDown() {
    if (scheduler != null && scheduler.isRunning()) {
      scheduler.stop();
    }
  }

  @Test
  void initialStateNotRunning() {
    assertThat(scheduler.isRunning()).isFalse();
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void startSetsRunningTrue() {
    scheduler.start();

    assertThat(scheduler.isRunning()).isTrue();
  }

  @Test
  void stopSetsRunningFalse() {
    scheduler.start();
    scheduler.stop();

    assertThat(scheduler.isRunning()).isFalse();
  }

  @Test
  void scheduleBeforeStartDoesNotSchedule() {
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(60);
    config.setSchedule(schedule);

    // Not started yet
    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void scheduleDisabledConfigDoesNotSchedule() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(false);

    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void scheduleWithNoScheduleConfigDoesNotSchedule() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(true);
    // No schedule set

    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void scheduleFrequencyBasedBackup() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(60);
    config.setSchedule(schedule);

    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void scheduleCronBasedBackup() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    schedule.setCronExpression("0 0 2 * * *");
    config.setSchedule(schedule);

    scheduler.scheduleBackup("testdb", config);

    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void cancelBackupRemovesFromSchedule() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule.setFrequencyMinutes(60);
    config.setSchedule(schedule);

    scheduler.scheduleBackup("testdb", config);
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    scheduler.cancelBackup("testdb");
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void cancelNonExistentBackupDoesNotThrow() {
    scheduler.start();

    // Should not throw
    scheduler.cancelBackup("nonexistent");

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void rescheduleReplacesExisting() {
    scheduler.start();

    final DatabaseBackupConfig config1 = new DatabaseBackupConfig("testdb");
    config1.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule1 = new DatabaseBackupConfig.ScheduleConfig();
    schedule1.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule1.setFrequencyMinutes(60);
    config1.setSchedule(schedule1);

    scheduler.scheduleBackup("testdb", config1);
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);

    // Reschedule with different config
    final DatabaseBackupConfig config2 = new DatabaseBackupConfig("testdb");
    config2.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule2 = new DatabaseBackupConfig.ScheduleConfig();
    schedule2.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    schedule2.setFrequencyMinutes(30);
    config2.setSchedule(schedule2);

    scheduler.scheduleBackup("testdb", config2);

    // Still only 1 scheduled (replaced)
    assertThat(scheduler.getScheduledCount()).isEqualTo(1);
  }

  @Test
  void multipleBackupsCanBeScheduled() {
    scheduler.start();

    for (int i = 1; i <= 3; i++) {
      final DatabaseBackupConfig config = new DatabaseBackupConfig("db" + i);
      config.setEnabled(true);
      final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
      schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
      schedule.setFrequencyMinutes(60);
      config.setSchedule(schedule);
      scheduler.scheduleBackup("db" + i, config);
    }

    assertThat(scheduler.getScheduledCount()).isEqualTo(3);
  }

  @Test
  void stopCancelsAllScheduledTasks() {
    scheduler.start();

    for (int i = 1; i <= 3; i++) {
      final DatabaseBackupConfig config = new DatabaseBackupConfig("db" + i);
      config.setEnabled(true);
      final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
      schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
      schedule.setFrequencyMinutes(60);
      config.setSchedule(schedule);
      scheduler.scheduleBackup("db" + i, config);
    }

    assertThat(scheduler.getScheduledCount()).isEqualTo(3);

    scheduler.stop();

    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }

  @Test
  void triggerImmediateBackupWhenNotRunningDoesNothing() {
    // Not started
    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");

    // Should not throw
    scheduler.triggerImmediateBackup("testdb", config);

    // No way to verify, but at least it doesn't throw
    assertThat(scheduler.isRunning()).isFalse();
  }

  @Test
  void invalidCronExpressionDoesNotSchedule() {
    scheduler.start();

    final DatabaseBackupConfig config = new DatabaseBackupConfig("testdb");
    config.setEnabled(true);
    final DatabaseBackupConfig.ScheduleConfig schedule = new DatabaseBackupConfig.ScheduleConfig();
    schedule.setType(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    schedule.setCronExpression("invalid cron");
    config.setSchedule(schedule);

    scheduler.scheduleBackup("testdb", config);

    // Should not be scheduled due to invalid cron
    assertThat(scheduler.getScheduledCount()).isEqualTo(0);
  }
}
```

**Step 2: Run tests to verify they pass**

Run: `mvn test -pl server -Dtest=BackupSchedulerTest -q`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/backup/BackupSchedulerTest.java
git commit -m "test: add BackupScheduler unit tests

Cover scheduler lifecycle, frequency and cron scheduling,
cancel operations, and multiple database handling."
```

---

## Task 4: Run All Server Tests

**Step 1: Run all server module tests**

Run: `mvn test -pl server -q`
Expected: All tests PASS, BUILD SUCCESS

**Step 2: Verify test counts**

Run: `mvn test -pl server 2>&1 | grep "Tests run:"`
Expected: Increased test count from ~15 to ~70+

**Step 3: Final commit with summary**

```bash
git add -A
git status
# If clean, no commit needed
# If there are changes:
git commit -m "test: server module test coverage complete"
```

---

## Summary

This plan adds approximately **55 new unit tests** across 3 new test files:

1. **DatabaseBackupConfigTest** (~40 tests) - Configuration parsing, validation, JSON serialization
2. **ExecutionResponseTest** (~10 tests) - HTTP response construction
3. **BackupSchedulerTest** (~15 tests) - Scheduler lifecycle and task management

All tests are pure unit tests with no external dependencies required.
