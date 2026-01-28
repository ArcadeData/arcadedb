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

import java.time.LocalTime;

/**
 * Configuration for a specific database backup, can override server-level defaults.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseBackupConfig {
  private final String          databaseName;
  private       boolean         enabled     = true;
  private       String          runOnServer = "$leader";
  private       ScheduleConfig  schedule;
  private       RetentionConfig retention;

  public DatabaseBackupConfig(final String databaseName) {
    this.databaseName = databaseName;
  }

  public static DatabaseBackupConfig fromJSON(final String databaseName, final JSONObject json) {
    final DatabaseBackupConfig config = new DatabaseBackupConfig(databaseName);

    if (json.has("enabled"))
      config.enabled = json.getBoolean("enabled");

    if (json.has("runOnServer"))
      config.runOnServer = json.getString("runOnServer");

    if (json.has("schedule"))
      config.schedule = ScheduleConfig.fromJSON(json.getJSONObject("schedule"));

    if (json.has("retention"))
      config.retention = RetentionConfig.fromJSON(json.getJSONObject("retention"));

    return config;
  }

  public void mergeWithDefaults(final DatabaseBackupConfig defaults) {
    if (defaults == null)
      return;

    if (this.schedule == null)
      this.schedule = defaults.schedule;
    else if (defaults.schedule != null)
      this.schedule.mergeWithDefaults(defaults.schedule);

    if (this.retention == null)
      this.retention = defaults.retention;
    else if (defaults.retention != null)
      this.retention.mergeWithDefaults(defaults.retention);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public String getRunOnServer() {
    return runOnServer;
  }

  public void setRunOnServer(final String runOnServer) {
    this.runOnServer = runOnServer;
  }

  public ScheduleConfig getSchedule() {
    return schedule;
  }

  public void setSchedule(final ScheduleConfig schedule) {
    this.schedule = schedule;
  }

  public RetentionConfig getRetention() {
    return retention;
  }

  public void setRetention(final RetentionConfig retention) {
    this.retention = retention;
  }

  /**
   * Schedule configuration supporting frequency-based or CRON scheduling.
   */
  public static class ScheduleConfig {
    public enum Type {
      FREQUENCY, CRON
    }

    private Type      type             = Type.FREQUENCY;
    private int       frequencyMinutes = 60;
    private String    cronExpression;
    private LocalTime windowStart;
    private LocalTime windowEnd;

    public static ScheduleConfig fromJSON(final JSONObject json) {
      final ScheduleConfig config = new ScheduleConfig();

      if (json.has("type"))
        config.type = Type.valueOf(json.getString("type").toUpperCase());

      if (json.has("frequencyMinutes"))
        config.frequencyMinutes = json.getInt("frequencyMinutes");

      if (json.has("expression"))
        config.cronExpression = json.getString("expression");

      if (json.has("timeWindow")) {
        final JSONObject window = json.getJSONObject("timeWindow");
        if (window.has("start"))
          config.windowStart = LocalTime.parse(window.getString("start"));
        if (window.has("end"))
          config.windowEnd = LocalTime.parse(window.getString("end"));
      }

      return config;
    }

    public void mergeWithDefaults(final ScheduleConfig defaults) {
      // Type-specific fields are not merged, only window times
      if (this.windowStart == null)
        this.windowStart = defaults.windowStart;
      if (this.windowEnd == null)
        this.windowEnd = defaults.windowEnd;
    }

    public Type getType() {
      return type;
    }

    public void setType(final Type type) {
      this.type = type;
    }

    public int getFrequencyMinutes() {
      return frequencyMinutes;
    }

    public void setFrequencyMinutes(final int frequencyMinutes) {
      this.frequencyMinutes = frequencyMinutes;
    }

    public String getCronExpression() {
      return cronExpression;
    }

    public void setCronExpression(final String cronExpression) {
      this.cronExpression = cronExpression;
    }

    public LocalTime getWindowStart() {
      return windowStart;
    }

    public void setWindowStart(final LocalTime windowStart) {
      this.windowStart = windowStart;
    }

    public LocalTime getWindowEnd() {
      return windowEnd;
    }

    public void setWindowEnd(final LocalTime windowEnd) {
      this.windowEnd = windowEnd;
    }

    public boolean hasTimeWindow() {
      return windowStart != null && windowEnd != null;
    }
  }

  /**
   * Retention configuration supporting tiered retention policies.
   */
  public static class RetentionConfig {
    private int          maxFiles = 10;
    private TieredConfig tiered;

    public static RetentionConfig fromJSON(final JSONObject json) {
      final RetentionConfig config = new RetentionConfig();

      if (json.has("maxFiles"))
        config.maxFiles = json.getInt("maxFiles");

      if (json.has("tiered"))
        config.tiered = TieredConfig.fromJSON(json.getJSONObject("tiered"));

      return config;
    }

    public void mergeWithDefaults(final RetentionConfig defaults) {
      if (this.tiered == null)
        this.tiered = defaults.tiered;
    }

    public int getMaxFiles() {
      return maxFiles;
    }

    public void setMaxFiles(final int maxFiles) {
      this.maxFiles = maxFiles;
    }

    public TieredConfig getTiered() {
      return tiered;
    }

    public void setTiered(final TieredConfig tiered) {
      this.tiered = tiered;
    }

    public boolean hasTieredRetention() {
      return tiered != null;
    }
  }

  /**
   * Tiered retention configuration (hourly/daily/weekly/monthly/yearly).
   */
  public static class TieredConfig {
    private int hourly  = 24;
    private int daily   = 7;
    private int weekly  = 4;
    private int monthly = 12;
    private int yearly  = 3;

    public static TieredConfig fromJSON(final JSONObject json) {
      final TieredConfig config = new TieredConfig();

      if (json.has("hourly"))
        config.hourly = json.getInt("hourly");
      if (json.has("daily"))
        config.daily = json.getInt("daily");
      if (json.has("weekly"))
        config.weekly = json.getInt("weekly");
      if (json.has("monthly"))
        config.monthly = json.getInt("monthly");
      if (json.has("yearly"))
        config.yearly = json.getInt("yearly");

      return config;
    }

    public int getHourly() {
      return hourly;
    }

    public void setHourly(final int hourly) {
      this.hourly = hourly;
    }

    public int getDaily() {
      return daily;
    }

    public void setDaily(final int daily) {
      this.daily = daily;
    }

    public int getWeekly() {
      return weekly;
    }

    public void setWeekly(final int weekly) {
      this.weekly = weekly;
    }

    public int getMonthly() {
      return monthly;
    }

    public void setMonthly(final int monthly) {
      this.monthly = monthly;
    }

    public int getYearly() {
      return yearly;
    }

    public void setYearly(final int yearly) {
      this.yearly = yearly;
    }
  }
}
