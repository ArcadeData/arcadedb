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
package com.arcadedb.server;

/**
 * A plugin refused a change of a declared server setting (see {@link ServerPlugin#checkServerSettingChange}). The message is
 * the plugin's own wording, meant for the operator who asked for the change. It is an {@link IllegalArgumentException} so
 * every surface that already answers a rejected argument keeps doing so, and a type of its own so a surface that words its
 * refusals separately from the engine's (the MCP tool) can tell it from an {@link IllegalArgumentException} the store raised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerSettingRefusedException extends IllegalArgumentException {
  public ServerSettingRefusedException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
