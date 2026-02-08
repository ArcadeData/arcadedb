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
package com.arcadedb.server.ha;

/**
 * Categories of exceptions that can occur during replication.
 * Each category drives a different recovery strategy.
 */
public enum ExceptionCategory {
  /**
   * Temporary network issues (timeouts, connection resets).
   * Recovery: Quick retry with exponential backoff.
   */
  TRANSIENT_NETWORK("Transient Network Failure"),

  /**
   * Leader changed, need to find new leader.
   * Recovery: Immediate leader discovery, no backoff.
   */
  LEADERSHIP_CHANGE("Leadership Change"),

  /**
   * Protocol version mismatch or corrupted data.
   * Recovery: Fail fast, no retry.
   */
  PROTOCOL_ERROR("Protocol Error"),

  /**
   * Uncategorized errors.
   * Recovery: Conservative retry with longer delays.
   */
  UNKNOWN("Unknown Error");

  private final String displayName;

  ExceptionCategory(String displayName) {
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }
}
