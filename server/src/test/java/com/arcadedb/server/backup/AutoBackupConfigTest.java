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
package com.arcadedb.server.backup;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AutoBackupConfigTest {

  @Test
  void testParseMinimalConfig() {
    final String json = """
        {
          "version": 1,
          "enabled": true,
          "backupDirectory": "./backups"
        }
        """;

    final AutoBackupConfig config = AutoBackupConfig.fromJSON(new JSONObject(json));

    assertThat(config.getVersion()).isEqualTo(1);
    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getBackupDirectory()).isEqualTo("./backups");
    assertThat(config.getDefaults()).isNull();
    assertThat(config.getDatabases()).isEmpty();
  }

  @Test
  void testParseFullConfig() {
    final String json = """
        {
          "version": 1,
          "enabled": true,
          "backupDirectory": "./backups",
          "defaults": {
            "enabled": true,
            "runOnServer": "$leader",
            "schedule": {
              "type": "frequency",
              "frequencyMinutes": 60,
              "timeWindow": {
                "start": "02:00",
                "end": "04:00"
              }
            },
            "retention": {
              "maxFiles": 10,
              "tiered": {
                "hourly": 24,
                "daily": 7,
                "weekly": 4,
                "monthly": 12,
                "yearly": 3
              }
            }
          },
          "databases": {
            "production": {
              "runOnServer": "replica1",
              "schedule": {
                "type": "cron",
                "expression": "0 0 2 * * ?"
              }
            }
          }
        }
        """;

    final AutoBackupConfig config = AutoBackupConfig.fromJSON(new JSONObject(json));

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getBackupDirectory()).isEqualTo("./backups");

    // Check defaults
    final DatabaseBackupConfig defaults = config.getDefaults();
    assertThat(defaults).isNotNull();
    assertThat(defaults.isEnabled()).isTrue();
    assertThat(defaults.getRunOnServer()).isEqualTo("$leader");

    // Check schedule
    final DatabaseBackupConfig.ScheduleConfig schedule = defaults.getSchedule();
    assertThat(schedule.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.FREQUENCY);
    assertThat(schedule.getFrequencyMinutes()).isEqualTo(60);
    assertThat(schedule.getWindowStart()).isEqualTo(LocalTime.of(2, 0));
    assertThat(schedule.getWindowEnd()).isEqualTo(LocalTime.of(4, 0));

    // Check retention
    final DatabaseBackupConfig.RetentionConfig retention = defaults.getRetention();
    assertThat(retention.getMaxFiles()).isEqualTo(10);
    assertThat(retention.hasTieredRetention()).isTrue();

    final DatabaseBackupConfig.TieredConfig tiered = retention.getTiered();
    assertThat(tiered.getHourly()).isEqualTo(24);
    assertThat(tiered.getDaily()).isEqualTo(7);
    assertThat(tiered.getWeekly()).isEqualTo(4);
    assertThat(tiered.getMonthly()).isEqualTo(12);
    assertThat(tiered.getYearly()).isEqualTo(3);

    // Check database-specific config
    assertThat(config.getDatabases()).containsKey("production");
    final DatabaseBackupConfig prodConfig = config.getDatabases().get("production");
    assertThat(prodConfig.getRunOnServer()).isEqualTo("replica1");
    assertThat(prodConfig.getSchedule().getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    assertThat(prodConfig.getSchedule().getCronExpression()).isEqualTo("0 0 2 * * ?");
  }

  @Test
  void testGetEffectiveConfigWithDefaults() {
    final String json = """
        {
          "version": 1,
          "enabled": true,
          "backupDirectory": "./backups",
          "defaults": {
            "enabled": true,
            "runOnServer": "$leader",
            "schedule": {
              "type": "frequency",
              "frequencyMinutes": 120
            },
            "retention": {
              "maxFiles": 5
            }
          }
        }
        """;

    final AutoBackupConfig config = AutoBackupConfig.fromJSON(new JSONObject(json));

    // Get effective config for an unknown database - should use defaults
    final DatabaseBackupConfig effective = config.getEffectiveConfig("unknown-db");

    assertThat(effective.getDatabaseName()).isEqualTo("unknown-db");
    assertThat(effective.isEnabled()).isTrue();
    assertThat(effective.getRunOnServer()).isEqualTo("$leader");
    assertThat(effective.getSchedule().getFrequencyMinutes()).isEqualTo(120);
    assertThat(effective.getRetention().getMaxFiles()).isEqualTo(5);
  }

  @Test
  void testGetEffectiveConfigWithOverrides() {
    final String json = """
        {
          "version": 1,
          "enabled": true,
          "backupDirectory": "./backups",
          "defaults": {
            "enabled": true,
            "runOnServer": "$leader",
            "schedule": {
              "type": "frequency",
              "frequencyMinutes": 120
            },
            "retention": {
              "maxFiles": 5
            }
          },
          "databases": {
            "mydb": {
              "runOnServer": "server1",
              "schedule": {
                "type": "frequency",
                "frequencyMinutes": 30
              }
            }
          }
        }
        """;

    final AutoBackupConfig config = AutoBackupConfig.fromJSON(new JSONObject(json));

    // Get effective config for mydb - should merge with defaults
    final DatabaseBackupConfig effective = config.getEffectiveConfig("mydb");

    assertThat(effective.getDatabaseName()).isEqualTo("mydb");
    assertThat(effective.getRunOnServer()).isEqualTo("server1"); // Overridden
    assertThat(effective.getSchedule().getFrequencyMinutes()).isEqualTo(30); // Overridden
    // Retention should come from defaults since not overridden
    assertThat(effective.getRetention().getMaxFiles()).isEqualTo(5);
  }

  @Test
  void testCreateDefault() {
    final AutoBackupConfig config = AutoBackupConfig.createDefault();

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getBackupDirectory()).isEqualTo(AutoBackupConfig.DEFAULT_BACKUP_DIR);
    assertThat(config.getDefaults()).isNotNull();
    assertThat(config.getDefaults().getSchedule().getFrequencyMinutes()).isEqualTo(AutoBackupConfig.DEFAULT_FREQUENCY);
    assertThat(config.getDefaults().getRetention().getMaxFiles()).isEqualTo(AutoBackupConfig.DEFAULT_MAX_FILES);
  }

  @Test
  void testDisabledConfig() {
    final String json = """
        {
          "version": 1,
          "enabled": false,
          "backupDirectory": "./backups"
        }
        """;

    final AutoBackupConfig config = AutoBackupConfig.fromJSON(new JSONObject(json));
    assertThat(config.isEnabled()).isFalse();
  }

  @Test
  void testCronScheduleConfig() {
    final String json = """
        {
          "version": 1,
          "enabled": true,
          "backupDirectory": "./backups",
          "defaults": {
            "schedule": {
              "type": "cron",
              "expression": "0 30 3 * * MON-FRI"
            }
          }
        }
        """;

    final AutoBackupConfig config = AutoBackupConfig.fromJSON(new JSONObject(json));
    final DatabaseBackupConfig.ScheduleConfig schedule = config.getDefaults().getSchedule();

    assertThat(schedule.getType()).isEqualTo(DatabaseBackupConfig.ScheduleConfig.Type.CRON);
    assertThat(schedule.getCronExpression()).isEqualTo("0 30 3 * * MON-FRI");
  }
}
