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
  /**
   * Bounds for the per-database compression overrides. They repeat the ones enforced by
   * {@code com.arcadedb.integration.backup.BackupSettings} and by the matching {@code GlobalConfiguration} entries,
   * because the server module deliberately does not depend on the integration module at compile time - the backup is
   * reached reflectively so that a distribution without it still starts. {@code compressionOverrideBoundsMatchTheBackupApi}
   * in {@code DatabaseBackupConfigTest} fails if the two ever drift.
   */
  public static final int MIN_COMPRESSION_LEVEL   = 0;
  public static final int MAX_COMPRESSION_LEVEL   = 9;
  public static final int MIN_COMPRESSION_THREADS = -1;
  public static final int MAX_COMPRESSION_THREADS = 256;
  public static final int MIN_MAX_MB_PER_SECOND   = 0;

  private final String          databaseName;
  private       boolean         enabled     = true;
  private       String          runOnServer = "$leader";
  private       ScheduleConfig  schedule;
  private       RetentionConfig retention;
  /**
   * Deflate level, 0 (store) to 9 (smallest). {@code null} means "defer to
   * {@code GlobalConfiguration.BACKUP_COMPRESSION_LEVEL}", the same convention {@code BackupSettings} uses, which is
   * what keeps a {@code backup.json} written before this setting existed behaving exactly as it did.
   */
  private       Integer         compressionLevel;
  /**
   * Compression threads: -1 automatic, 0 the legacy single-threaded writer, N a pool of N. {@code null} defers to
   * {@code GlobalConfiguration.BACKUP_COMPRESSION_THREADS}.
   */
  private       Integer         compressionThreads;
  /**
   * Read-side rate cap in MB/s, 0 for unlimited. {@code null} defers to
   * {@code GlobalConfiguration.BACKUP_MAX_MB_PER_SECOND}.
   */
  private       Integer         maxMBPerSecond;

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

    // ABSENT MEANS "DEFER TO THE GLOBAL CONFIGURATION", SO AN EXISTING backup.json KEEPS ITS EXACT BEHAVIOUR
    if (json.has("compressionLevel"))
      config.compressionLevel = json.getInt("compressionLevel");

    if (json.has("compressionThreads"))
      config.compressionThreads = json.getInt("compressionThreads");

    if (json.has("maxMBPerSecond"))
      config.maxMBPerSecond = json.getInt("maxMBPerSecond");

    // Validate the configuration
    config.validate();

    return config;
  }

  /**
   * Validates the configuration and throws IllegalArgumentException if invalid.
   */
  public void validate() {
    if (schedule != null)
      schedule.validate();

    if (retention != null)
      retention.validate();

    // REFUSED HERE, AT LOAD TIME, RATHER THAN INSIDE THE Backup SETTER AT 3AM: AN OUT-OF-RANGE VALUE IN backup.json IS
    // A CONFIGURATION MISTAKE AND THE OPERATOR SHOULD HEAR ABOUT IT WHEN THE SERVER READS THE FILE
    checkRange("compressionLevel", compressionLevel, MIN_COMPRESSION_LEVEL, MAX_COMPRESSION_LEVEL);
    checkRange("compressionThreads", compressionThreads, MIN_COMPRESSION_THREADS, MAX_COMPRESSION_THREADS);
    checkRange("maxMBPerSecond", maxMBPerSecond, MIN_MAX_MB_PER_SECOND, Integer.MAX_VALUE);
  }

  /**
   * The message is word-for-word the one {@code BackupSettings.checkIntSetting} raises for the same setting, so the
   * two places that can refuse a compression value - this one when {@code backup.json} is read, that one when the
   * value reaches the {@code Backup} - read identically in the log.
   */
  private static void checkRange(final String name, final Integer value, final int min, final int max) {
    if (value != null && (value < min || value > max))
      throw new IllegalArgumentException(
          "Backup setting '%s' must be between %d and %d, found %d".formatted(name, min, max, value));
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

    // A DATABASE THAT SET NOTHING INHERITS THE SERVER DEFAULT; ONE THAT SET A VALUE KEEPS IT. STILL null AFTERWARDS
    // MEANS NEITHER LEVEL EXPRESSED AN OPINION AND THE GlobalConfiguration DEFAULT APPLIES
    if (this.compressionLevel == null)
      this.compressionLevel = defaults.compressionLevel;
    if (this.compressionThreads == null)
      this.compressionThreads = defaults.compressionThreads;
    if (this.maxMBPerSecond == null)
      this.maxMBPerSecond = defaults.maxMBPerSecond;
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

  public Integer getCompressionLevel() {
    return compressionLevel;
  }

  public void setCompressionLevel(final Integer compressionLevel) {
    this.compressionLevel = compressionLevel;
  }

  public Integer getCompressionThreads() {
    return compressionThreads;
  }

  public void setCompressionThreads(final Integer compressionThreads) {
    this.compressionThreads = compressionThreads;
  }

  public Integer getMaxMBPerSecond() {
    return maxMBPerSecond;
  }

  public void setMaxMBPerSecond(final Integer maxMBPerSecond) {
    this.maxMBPerSecond = maxMBPerSecond;
  }

  /**
   * Converts this configuration to a JSON object.
   */
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("enabled", enabled);
    json.put("runOnServer", runOnServer);

    if (schedule != null)
      json.put("schedule", schedule.toJSON());

    if (retention != null)
      json.put("retention", retention.toJSON());

    // ONLY WHAT WAS ACTUALLY SET IS WRITTEN BACK: EMITTING A RESOLVED DEFAULT WOULD FREEZE TODAY'S GLOBAL VALUE INTO
    // THE FILE AND THE DATABASE WOULD STOP FOLLOWING THE SERVER SETTING
    if (compressionLevel != null)
      json.put("compressionLevel", compressionLevel);

    if (compressionThreads != null)
      json.put("compressionThreads", compressionThreads);

    if (maxMBPerSecond != null)
      json.put("maxMBPerSecond", maxMBPerSecond);

    return json;
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

    /**
     * Validates the schedule configuration.
     */
    public void validate() {
      if (type == Type.FREQUENCY) {
        if (frequencyMinutes < 1)
          throw new IllegalArgumentException("Backup frequency must be at least 1 minute, got: " + frequencyMinutes);
        if (frequencyMinutes > 525600) // 1 year in minutes
          throw new IllegalArgumentException("Backup frequency cannot exceed 1 year (525600 minutes), got: " + frequencyMinutes);
      } else if (type == Type.CRON) {
        if (cronExpression == null || cronExpression.trim().isEmpty())
          throw new IllegalArgumentException("CRON expression is required when schedule type is CRON");
        // Validate CRON expression by parsing it
        try {
          new CronScheduleParser(cronExpression);
        } catch (final Exception e) {
          throw new IllegalArgumentException("Invalid CRON expression '" + cronExpression + "': " + e.getMessage(), e);
        }
      }
    }

    /**
     * Converts this schedule configuration to a JSON object.
     */
    public JSONObject toJSON() {
      final JSONObject json = new JSONObject();
      json.put("type", type.name().toLowerCase());

      if (type == Type.FREQUENCY)
        json.put("frequencyMinutes", frequencyMinutes);
      else if (type == Type.CRON && cronExpression != null)
        json.put("expression", cronExpression);

      if (windowStart != null || windowEnd != null) {
        final JSONObject window = new JSONObject();
        if (windowStart != null)
          window.put("start", windowStart.toString());
        if (windowEnd != null)
          window.put("end", windowEnd.toString());
        json.put("timeWindow", window);
      }

      return json;
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

    /**
     * Validates the retention configuration.
     */
    public void validate() {
      if (maxFiles < 1)
        throw new IllegalArgumentException("Max backup files must be at least 1, got: " + maxFiles);
      if (maxFiles > 10000)
        throw new IllegalArgumentException("Max backup files cannot exceed 10000, got: " + maxFiles);

      if (tiered != null)
        tiered.validate();
    }

    /**
     * Converts this retention configuration to a JSON object.
     */
    public JSONObject toJSON() {
      final JSONObject json = new JSONObject();
      json.put("maxFiles", maxFiles);

      if (tiered != null)
        json.put("tiered", tiered.toJSON());

      return json;
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

    /**
     * Validates the tiered retention configuration.
     */
    public void validate() {
      if (hourly < 0)
        throw new IllegalArgumentException("Hourly retention cannot be negative: " + hourly);
      if (daily < 0)
        throw new IllegalArgumentException("Daily retention cannot be negative: " + daily);
      if (weekly < 0)
        throw new IllegalArgumentException("Weekly retention cannot be negative: " + weekly);
      if (monthly < 0)
        throw new IllegalArgumentException("Monthly retention cannot be negative: " + monthly);
      if (yearly < 0)
        throw new IllegalArgumentException("Yearly retention cannot be negative: " + yearly);

      // Limit maximum values to prevent unbounded memory usage
      final int maxRetention = 1000;
      if (hourly > maxRetention || daily > maxRetention || weekly > maxRetention ||
          monthly > maxRetention || yearly > maxRetention)
        throw new IllegalArgumentException("Tiered retention values cannot exceed " + maxRetention);
    }

    /**
     * Converts this tiered retention configuration to a JSON object.
     */
    public JSONObject toJSON() {
      final JSONObject json = new JSONObject();
      json.put("hourly", hourly);
      json.put("daily", daily);
      json.put("weekly", weekly);
      json.put("monthly", monthly);
      json.put("yearly", yearly);
      return json;
    }
  }
}
