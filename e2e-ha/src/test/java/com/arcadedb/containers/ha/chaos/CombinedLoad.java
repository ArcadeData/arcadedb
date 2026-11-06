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

package com.arcadedb.containers.ha.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fans every {@link LoadGenerator} call out to a primary load and to further loads running beside it. {@link #acked()} reports
 * the primary only, because the runner uses it as the ledger progress signal.
 */
public final class CombinedLoad implements LoadGenerator {
  private final List<LoadGenerator> members = new ArrayList<>();
  private final AtomicBoolean       closed  = new AtomicBoolean();

  public CombinedLoad(final LoadGenerator primary, final LoadGenerator... others) {
    members.add(primary);
    members.addAll(List.of(others));
  }

  @Override
  public void start() {
    for (final LoadGenerator member : members)
      member.start();
  }

  @Override
  public void quiesce() throws InterruptedException {
    for (final LoadGenerator member : members)
      member.quiesce();
  }

  @Override
  public void resume() {
    for (int i = members.size() - 1; i >= 0; i--)
      members.get(i).resume();
  }

  @Override
  public long acked() {
    return members.get(0).acked();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true))
      return;
    RuntimeException first = null;
    for (final LoadGenerator member : members)
      try {
        member.close();
      } catch (final RuntimeException e) {
        if (first == null)
          first = e;
        else
          first.addSuppressed(e);
      }
    if (first != null)
      throw first;
  }
}
