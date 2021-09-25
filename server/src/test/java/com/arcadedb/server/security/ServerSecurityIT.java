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
 */
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ServerSecurityIT {

  private static final String PASSWORD = "dD5ed08c";

  @Test
  void shouldCreateDefaultRootUserAndPersistsSecurityConfigurationFromSetting() throws IOException {
    //cleanup
    final Path securityConfPath = Paths.get("./target", SecurityUserFileRepository.FILE_NAME);
    Files.deleteIfExists(securityConfPath);

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);
    try {
      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
      security.startService();
      security.loadUsers();

      File securityConf = securityConfPath.toFile();

      Assertions.assertTrue(securityConf.exists());

      SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

      final List<JSONObject> jsonl = repository.load();

      Assertions.assertEquals(1, jsonl.size());
      Assertions.assertEquals("root", jsonl.get(0).getString("name"));
      passwordShouldMatch(security, PASSWORD, jsonl.get(0).getString("password"));

    } finally {
      GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    }
  }

  @Test
  void shouldCreateDefaultRootUserAndPersistsSecurityConfigurationFromUserInput() throws IOException {
    //cleanup
    final Path securityConfPath = Paths.get("./target", SecurityUserFileRepository.FILE_NAME);
    Files.deleteIfExists(securityConfPath);

    if (System.console() != null) {
      System.console().writer().println("dD5ed08c\r\ndD5ed08c\n");
    } else {
      final InputStream is = new ByteArrayInputStream("dD5ed08c\r\ndD5ed08c\n".getBytes());
      System.setIn(is);
    }

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.startService();
    security.loadUsers();

    File securityConf = securityConfPath.toFile();

    Assertions.assertTrue(securityConf.exists());

    SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

    final List<JSONObject> jsonl = repository.load();

    Assertions.assertEquals(1, jsonl.size());
    Assertions.assertEquals("root", jsonl.get(0).getString("name"));
    passwordShouldMatch(security, PASSWORD, jsonl.get(0).getString("password"));
  }

  @Test
  void shouldLoadProvidedSecurityConfiguration() throws IOException {
    final Path securityConfPath = Paths.get("./target", SecurityUserFileRepository.FILE_NAME);
    Files.deleteIfExists(securityConfPath);

    GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(PASSWORD);
    try {
      SecurityUserFileRepository repository = new SecurityUserFileRepository("./target");

      final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

      final JSONObject json = new JSONObject().put("name", "providedUser").put("password", security.encodePassword("MyPassword12345"))
          .put("databases", new JSONObject());

      repository.save(Collections.singletonList(json));

      //when
      security.startService();
      security.loadUsers();

      Assertions.assertTrue(security.existsUser("providedUser"));
      Assertions.assertFalse(security.existsUser("root"));
      passwordShouldMatch(security, "MyPassword12345", security.getUser("providedUser").getPassword());
    } finally {
      GlobalConfiguration.SERVER_ROOT_PASSWORD.setValue(null);
    }
  }

  @Test
  public void checkPasswordHash() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");
    security.startService();

    Assertions.assertEquals("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=",
        security.encodePassword("ThisIsATest", "ThisIsTheSalt"));
    Assertions.assertEquals("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=",
        security.encodePassword("ThisIsATest", "ThisIsTheSalt"));

    for (int i = 0; i < 1000000; ++i) {
      Assertions.assertFalse(ServerSecurity.generateRandomSalt().contains("$"));
    }

    security.stopService();
  }

  private void passwordShouldMatch(final ServerSecurity security, String password, String expectedHash) {
    Assertions.assertTrue(security.passwordMatch(password, expectedHash));
  }
}
