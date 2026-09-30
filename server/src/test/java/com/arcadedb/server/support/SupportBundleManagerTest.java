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
package com.arcadedb.server.support;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SupportBundleManagerTest {

  @Test
  void aPreviewLivesFifteenMinutes() throws Exception {
    final AtomicLong now = new AtomicLong(1_000_000L);
    try (final SupportBundleManager manager = new SupportBundleManager(now::get, SupportBundleManager.TTL_MS)) {
      final SupportBundleManager.Bundle bundle = manager.create();
      Files.writeString(bundle.getDirectory().resolve("diagnostics.json"), "{}");
      assertThat(bundle.getId()).hasSize(32);
      assertThat(bundle.getExpiresAt()).isEqualTo(1_000_000L + 15 * 60_000L);

      now.addAndGet(14 * 60_000L);
      assertThat(manager.get(bundle.getId())).isSameAs(bundle);
      assertThat(bundle.getDirectory()).exists();

      now.addAndGet(2 * 60_000L);
      assertThatThrownBy(() -> manager.get(bundle.getId())).isInstanceOfSatisfying(SupportException.class, e -> {
        assertThat(e.getCode()).isEqualTo("preview_not_found");
        assertThat(e.getStudioStatus()).isEqualTo(404);
      });
      // the expired directory and its files are gone
      assertThat(bundle.getDirectory()).doesNotExist();
      assertThat(manager.size()).isZero();
    }
  }

  @Test
  void expiredPreviewsAreDeletedWhenAnotherIsCreated() throws Exception {
    final AtomicLong now = new AtomicLong(0L);
    try (final SupportBundleManager manager = new SupportBundleManager(now::get, 1000L)) {
      final Path first = manager.create().getDirectory();
      now.addAndGet(2000L);
      final SupportBundleManager.Bundle second = manager.create();
      assertThat(first).doesNotExist();
      assertThat(manager.ids()).containsExactly(second.getId());
    }
  }

  @Test
  void closeDeletesEveryPreview() throws Exception {
    final SupportBundleManager manager = new SupportBundleManager();
    final Path a = manager.create().getDirectory();
    final Path b = manager.create().getDirectory();
    Files.writeString(a.resolve("logs.zip"), "x");
    manager.close();
    assertThat(a).doesNotExist();
    assertThat(b).doesNotExist();
    assertThat(manager.size()).isZero();
    assertThatThrownBy(manager::create).isInstanceOfSatisfying(SupportException.class, e -> {
      assertThat(e.getCode()).isEqualTo("support_stopped");
      assertThat(e.getStudioStatus()).isEqualTo(503);
    });
  }

  @Test
  void aLeasedPreviewSurvivesItsExpiryUntilTheLeaseCloses() throws Exception {
    final AtomicLong now = new AtomicLong(0L);
    try (final SupportBundleManager manager = new SupportBundleManager(now::get, 1000L)) {
      final SupportBundleManager.Bundle bundle = manager.create();
      Files.writeString(bundle.getDirectory().resolve("logs.zip"), "x");
      final SupportBundleManager.Lease lease = manager.lease(bundle.getId());

      // A slow upload outlives the 15 minutes: neither the cleaner nor another call may delete the files under it
      now.addAndGet(5000L);
      manager.purgeExpired();
      assertThat(bundle.getDirectory().resolve("logs.zip")).exists();
      assertThat(manager.ids()).containsExactly(bundle.getId());

      // ... and it goes as soon as the last lease is released
      lease.close();
      assertThat(bundle.getDirectory()).doesNotExist();
      assertThat(manager.size()).isZero();
      // closing twice is harmless
      lease.close();
    }
  }

  @Test
  void aLeasedPreviewIsNeverTheEvictionVictim() throws Exception {
    final AtomicLong now = new AtomicLong(0L);
    try (final SupportBundleManager manager = new SupportBundleManager(now::get, SupportBundleManager.TTL_MS)) {
      final SupportBundleManager.Bundle oldest = manager.create();
      final SupportBundleManager.Lease lease = manager.lease(oldest.getId());
      for (int i = 1; i < SupportBundleManager.MAX_BUNDLES + 3; i++) {
        now.incrementAndGet();
        manager.create();
      }
      // The oldest is the one being sent: another idle one was evicted instead
      assertThat(oldest.getDirectory()).exists();
      assertThat(manager.ids()).contains(oldest.getId());
      lease.close();
    }
  }

  @Test
  void theLimitIsExceededRatherThanDeletingAPreviewInUse() throws Exception {
    try (final SupportBundleManager manager = new SupportBundleManager()) {
      final java.util.List<SupportBundleManager.Lease> leases = new java.util.ArrayList<>();
      for (int i = 0; i < SupportBundleManager.MAX_BUNDLES; i++)
        leases.add(manager.lease(manager.create().getId()));
      // Every preview is being sent: creating one more must terminate and must not delete any of them
      final SupportBundleManager.Bundle extra = manager.create();
      assertThat(manager.size()).isEqualTo(SupportBundleManager.MAX_BUNDLES + 1);
      assertThat(extra.getDirectory()).exists();
      leases.forEach(SupportBundleManager.Lease::close);
    }
  }

  @Test
  void leasingAnUnknownOrRemovedPreviewIsPreviewNotFound() throws Exception {
    try (final SupportBundleManager manager = new SupportBundleManager()) {
      assertThatThrownBy(() -> manager.lease("nope")).isInstanceOfSatisfying(SupportException.class,
          e -> assertThat(e.getCode()).isEqualTo("preview_not_found"));
      final SupportBundleManager.Bundle bundle = manager.create();
      manager.remove(bundle.getId());
      assertThatThrownBy(() -> manager.lease(bundle.getId())).isInstanceOf(SupportException.class);
    }
  }

  @Test
  void theNumberOfPreviewsIsBounded() throws Exception {
    final AtomicLong now = new AtomicLong(0L);
    try (final SupportBundleManager manager = new SupportBundleManager(now::get, SupportBundleManager.TTL_MS)) {
      final Path oldest = manager.create().getDirectory();
      for (int i = 1; i < SupportBundleManager.MAX_BUNDLES + 3; i++) {
        now.incrementAndGet();
        manager.create();
      }
      assertThat(manager.size()).isLessThanOrEqualTo(SupportBundleManager.MAX_BUNDLES);
      assertThat(oldest).doesNotExist();
    }
  }

  @Test
  void removeAndUnknownIds() throws Exception {
    try (final SupportBundleManager manager = new SupportBundleManager()) {
      final SupportBundleManager.Bundle bundle = manager.create();
      manager.remove(bundle.getId());
      assertThat(bundle.getDirectory()).doesNotExist();
      manager.remove("nope");
      assertThatThrownBy(() -> manager.get("nope")).isInstanceOf(SupportException.class);
      assertThatThrownBy(() -> manager.get(null)).isInstanceOf(SupportException.class);
    }
  }
}
