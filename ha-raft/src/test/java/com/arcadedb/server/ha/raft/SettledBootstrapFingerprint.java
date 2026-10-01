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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.BootstrapFingerprint;
import com.arcadedb.database.LocalDatabase;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bootstrap fingerprint of an OPEN test database, taken only once its committed pages have reached the disk
 * (issue #8177).
 * <p>
 * {@link BootstrapFingerprint#compute(File)} hashes the files as they are on disk, and a commit hands its pages to
 * the asynchronous flush thread rather than writing them itself. A test that commits in its setup and fingerprints
 * the directory straight away can therefore hash the files a moment before the flush lands, while the state machine
 * fingerprints the same directory a moment after: two different digests of one copy, and the apply takes the
 * mismatch arm on a peer whose copy IS the baseline. Draining the flush queue first makes the test's sample and the
 * state machine's recomputation read the same bytes.
 */
final class SettledBootstrapFingerprint {

  private SettledBootstrapFingerprint() {
  }

  static String of(final LocalDatabase database) {
    assertThat(database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database))
        .as("the committed pages of '%s' reached the disk before its fingerprint is taken", database.getName())
        .isTrue();
    return BootstrapFingerprint.compute(new File(database.getDatabasePath()));
  }
}
