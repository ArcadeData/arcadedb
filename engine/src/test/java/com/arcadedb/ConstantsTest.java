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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConstantsTest {

  @Test
  void productConstantsAreSet() {
    assertThat(Constants.PRODUCT).isEqualTo("ArcadeDB");
    assertThat(Constants.URL).isEqualTo("https://arcadedb.com");
    assertThat(Constants.COPYRIGHT).isNotEmpty();
  }

  @Test
  void getRawVersionReturnsVersion() {
    final String version = Constants.getRawVersion();
    assertThat(version).isNotNull();
    assertThat(version).isNotEmpty();
  }

  @Test
  void getVersionMajorReturnsPositiveNumber() {
    final int major = Constants.getVersionMajor();
    assertThat(major).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getVersionMinorReturnsPositiveNumber() {
    final int minor = Constants.getVersionMinor();
    assertThat(minor).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getVersionHotfixReturnsNonNegativeNumber() {
    final int hotfix = Constants.getVersionHotfix();
    assertThat(hotfix).isGreaterThanOrEqualTo(0);
  }

  @Test
  void getVersionReturnsFormattedString() {
    final String version = Constants.getVersion();
    assertThat(version).isNotNull();
    assertThat(version).contains(Constants.getRawVersion());
  }

  @Test
  void isSnapshotReturnsBooleanBasedOnVersion() {
    final boolean isSnapshot = Constants.isSnapshot();
    final String rawVersion = Constants.getRawVersion();

    if (rawVersion.endsWith("SNAPSHOT")) {
      assertThat(isSnapshot).isTrue();
    } else {
      assertThat(isSnapshot).isFalse();
    }
  }

  @Test
  void getBranchReturnsNullOrValidBranch() {
    final String branch = Constants.getBranch();
    // Branch can be null if not set during build, or a valid string
    if (branch != null) {
      assertThat(branch).doesNotContain("${scmBranch}");
    }
  }

  @Test
  void getBuildNumberReturnsNullOrValidNumber() {
    final String buildNumber = Constants.getBuildNumber();
    // Build number can be null if not set during build, or a valid string
    if (buildNumber != null) {
      assertThat(buildNumber).doesNotContain("${buildNumber}");
    }
  }

  @Test
  void getTimestampReturnsNullOrValidTimestamp() {
    final String timestamp = Constants.getTimestamp();
    // Timestamp can be null if not set during build, or a valid string
    if (timestamp != null) {
      assertThat(timestamp).doesNotContain("${timestamp}");
    }
  }

  @Test
  void versionPartsAreConsistent() {
    final String rawVersion = Constants.getRawVersion();
    final int major = Constants.getVersionMajor();
    final int minor = Constants.getVersionMinor();

    // The raw version should start with major.minor
    assertThat(rawVersion).startsWith(major + "." + minor);
  }
}
