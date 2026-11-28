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
package com.arcadedb.mcp;

/**
 * A call the MCP layer itself refuses ("Tool 'x' is not available in the profile", "Read operations are not allowed by MCP
 * configuration", an administration call by a user that is not root): text this server words about the request, which names no
 * schema object and is what the caller needs to correct the call, so it is answered in every server mode. Any other
 * {@link SecurityException} - the engine's refusal of a record or a type, which names the schema objects of the database - is
 * engine text, which production mode conceals like it does for every other failure of a tool (see
 * {@link MCPToolArgumentException}). Telling the two apart by the JDK exception class would let an engine message through.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MCPRefusalException extends SecurityException {
  public MCPRefusalException(final String message) {
    super(message);
  }
}
