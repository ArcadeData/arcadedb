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
package com.arcadedb.server.plugin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ServerPlugin;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Refuses a runtime switch of {@code arcadedb.server.mode} to {@code production}, the way the Gremlin Server plugin does
 * while it runs with a channelizer that cannot conceal engine error text (issue #9749), and records every change it is
 * asked about. Lets the writers of a server setting be tested against the plugin hook without the Gremlin module.
 * <p>
 * Not auto-discovered: it is installed only by a test that names it in {@code SERVER_PLUGINS}.
 */
public class ServerSettingVetoTestPlugin implements ServerPlugin {
  public static final String REFUSAL = "refused by ServerSettingVetoTestPlugin";

  /** One change the plugin was asked about, with the value as the plugin received it. */
  public record Change(GlobalConfiguration setting, Object value) {
  }

  private final List<Change> changes = new CopyOnWriteArrayList<>();

  @Override
  public void startService() {
    // NOTHING TO START
  }

  @Override
  public void checkServerSettingChange(final GlobalConfiguration setting, final Object value) {
    changes.add(new Change(setting, value));
    if (setting == GlobalConfiguration.SERVER_MODE && "production".equals(value))
      throw new IllegalArgumentException(REFUSAL);
  }

  public List<Change> getChanges() {
    return changes;
  }
}
