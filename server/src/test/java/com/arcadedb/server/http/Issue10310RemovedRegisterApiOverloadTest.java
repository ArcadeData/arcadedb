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
package com.arcadedb.server.http;

import com.arcadedb.server.ServerPlugin;
import io.undertow.server.handlers.PathHandler;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10310: {@code ServerPlugin.registerAPI(HttpServer, PathHandler)} is gone. Nothing calls it any more, so the server
 * names a plugin that still declares it instead of starting it without its routes in silence.
 */
class Issue10310RemovedRegisterApiOverloadTest {

  @Test
  void serverPluginNoLongerDeclaresThePathHandlerOverload() {
    assertThatThrownBy(() -> ServerPlugin.class.getMethod("registerAPI", HttpServer.class, PathHandler.class))
        .isInstanceOf(NoSuchMethodException.class);
    assertThat(ServerPlugin.class.getMethods()).filteredOn(m -> m.getName().equals("registerAPI")).hasSize(1);
  }

  @Test
  void aPluginDeclaringTheRemovedOverloadIsDetected() {
    assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new LegacyPlugin())).isTrue();
  }

  @Test
  void anInheritedDeclarationIsDetected() {
    assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new LegacyPlugin() {
    })).isTrue();
    assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new LegacyInterfacePlugin())).isTrue();
  }

  @Test
  void aPluginOnThePluginRoutesOverloadOrWithNoRoutesIsNot() {
    assertThat(PluginPathHandler.declaresRemovedRegisterAPI(new PluginRoutesTestPlugin())).isFalse();
    final ServerPlugin noRoutes = () -> {
    };
    assertThat(PluginPathHandler.declaresRemovedRegisterAPI(noRoutes)).isFalse();
  }

  private static class LegacyPlugin implements ServerPlugin {
    @Override
    public void startService() {
      // NO-OP
    }

    public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
      // NO-OP
    }
  }

  private interface LegacyInterface {
    default void registerAPI(final HttpServer httpServer, final PathHandler routes) {
      // NO-OP
    }
  }

  private static final class LegacyInterfacePlugin implements ServerPlugin, LegacyInterface {
    @Override
    public void startService() {
      // NO-OP
    }
  }
}
