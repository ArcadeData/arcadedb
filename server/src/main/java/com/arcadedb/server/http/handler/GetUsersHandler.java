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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class GetUsersHandler extends AbstractServerHttpHandler {

  public GetUsersHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final ServerSecurity security = httpServer.getServer().getSecurity();
    final JSONArray usersArray = new JSONArray();

    for (final JSONObject userJson : security.usersToJSON()) {
      final JSONObject userInfo = new JSONObject();
      userInfo.put("name", userJson.getString("name"));
      // Never expose password hashes
      if (userJson.has("databases"))
        userInfo.put("databases", userJson.getJSONObject("databases"));
      else
        userInfo.put("databases", new JSONObject());
      usersArray.put(userInfo);
    }

    final JSONObject response = new JSONObject();
    response.put("result", usersArray);
    return new ExecutionResponse(200, response.toString());
  }
}
