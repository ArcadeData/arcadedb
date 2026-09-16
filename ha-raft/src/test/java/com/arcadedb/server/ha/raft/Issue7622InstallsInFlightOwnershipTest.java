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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7622: {@code INSTALLS_IN_FLIGHT} became load-bearing when the #7128 recovery-pass
 * skip guards in {@link SnapshotInstaller#recoverPendingSnapshotSwaps(Path)} started consulting it to decide
 * whether it is safe to delete a directory, but the registration itself was still a plain presence set with
 * no owner. Two overlapping installs of the same database shared one entry, and the {@code finally} of
 * whichever finished FIRST removed it unconditionally - clearing the guard while the other install was still
 * running, which is exactly the "delete a live install's directory" failure #7128 was filed about.
 * <p>
 * This test drives the two test-only registration hooks directly (they exercise the same
 * {@code registerInstallInFlight}/{@code releaseInstallInFlight} pair the real {@code install()} uses) to
 * simulate two overlapping installs of the same directory, and proves the guard stays up for as long as
 * either one is still "running".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7622InstallsInFlightOwnershipTest {

  @Test
  void guardStaysUpUntilEveryOverlappingInstallReleasesIt(@TempDir final Path databasesDir) throws Exception {
    final Path dbDir = databasesDir.resolve("mydb");
    final Path snapshotNew = dbDir.resolve(".snapshot-new");

    Files.createDirectories(snapshotNew);
    Files.writeString(dbDir.resolve(".snapshot-pending"), "");
    Files.writeString(dbDir.resolve("schema.json"), "{}");
    Files.writeString(snapshotNew.resolve("partial.dat"), "still-being-extracted");

    // Two overlapping installs of the same directory register independently, as install() does when a second
    // install joins before the first's bounded maintenance-slot wait has expired (see the class note on
    // SnapshotInstaller.INSTALLS_IN_FLIGHT).
    SnapshotInstaller.markInstallInFlightForTesting(dbDir);
    SnapshotInstaller.markInstallInFlightForTesting(dbDir);
    try {
      // The FIRST of the two finishes and releases its registration. Before the fix this cleared the ONLY
      // entry outright, even though the second install is still running.
      SnapshotInstaller.clearInstallInFlightForTesting(dbDir);

      SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);

      // The second (still-running) install's directory must survive: the guard has to stay up as long as ANY
      // overlapping install holds it, not just the first one to register.
      assertThat(snapshotNew).as("a still-running overlapping install's staging directory must not be deleted")
          .exists();
      assertThat(snapshotNew.resolve("partial.dat")).exists();
      assertThat(dbDir.resolve(".snapshot-pending")).exists();
    } finally {
      // The second (and last) install now releases its own registration.
      SnapshotInstaller.clearInstallInFlightForTesting(dbDir);
    }

    // With nothing left in flight, the same on-disk state - now genuinely orphaned - is cleaned up normally.
    SnapshotInstaller.recoverPendingSnapshotSwaps(databasesDir);
    assertThat(snapshotNew).doesNotExist();
    assertThat(dbDir.resolve(".snapshot-pending")).doesNotExist();
  }
}
