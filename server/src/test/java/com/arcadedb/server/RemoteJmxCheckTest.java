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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The startup check that reports a JVM whose remote JMX agent accepts connections without authentication. Such an agent hands
 * code execution inside the server process to whoever can reach its port, so the server must say so loudly however the agent
 * was configured: on the command line, in the file {@code com.sun.management.config.file} names, or in the JDK's own
 * {@code management.properties}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteJmxCheckTest {
  @TempDir
  Path tempDir;

  @Test
  void noRemoteAgentNoWarning() {
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(jvm())).isNull();
  }

  @Test
  void theFormerLauncherDefaultIsReported() {
    // What bin/server.sh, bin/server.bat and the Docker image used to pass to every server they started.
    final Properties props = jvm();
    props.setProperty("com.sun.management.jmxremote", "true");
    props.setProperty("com.sun.management.jmxremote.local.only", "false");
    props.setProperty("com.sun.management.jmxremote.authenticate", "false");
    props.setProperty("com.sun.management.jmxremote.ssl", "false");
    props.setProperty("com.sun.management.jmxremote.port", "9999");
    props.setProperty("com.sun.management.jmxremote.rmi.port", "9998");

    final String warning = RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props);
    assertThat(warning).isNotNull();
    assertThat(warning).contains("9999").contains("without authentication").contains("ARCADEDB_JMX");
  }

  @Test
  void authenticationIsOnUnlessDisabled() {
    final Properties props = jvm();
    props.setProperty("com.sun.management.jmxremote.port", "9999");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();

    props.setProperty("com.sun.management.jmxremote.authenticate", "true");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();

    props.setProperty("com.sun.management.jmxremote.authenticate", "TRUE");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();
  }

  @Test
  void anyValueOtherThanTrueDisablesAuthentication() {
    // The JDK agent reads the flag with Boolean.valueOf: "no", "0" or a typo all switch authentication off.
    final Properties props = jvm();
    props.setProperty("com.sun.management.jmxremote.port", "9999");
    props.setProperty("com.sun.management.jmxremote.authenticate", "no");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNotNull();

    // Nor is the value trimmed: "true " with a trailing blank is not "true" for the JDK, so authentication is off.
    props.setProperty("com.sun.management.jmxremote.authenticate", "true ");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNotNull();
  }

  @Test
  void onlyTheAgentActivationKeysStartTheAgent() throws IOException {
    // The JDK file opens an unauthenticated port, but the agent reads it only when config.file, jmxremote or jmxremote.port
    // is a system property: any other com.sun.management setting leaves the agent, and the port, off.
    final Path javaHome = tempDir.resolve("jdk-activation");
    final Path managementDir = javaHome.resolve("conf").resolve("management");
    Files.createDirectories(managementDir);
    Files.writeString(managementDir.resolve("management.properties"), """
        com.sun.management.jmxremote.port=7094
        com.sun.management.jmxremote.authenticate=false
        """);

    final Properties props = new Properties();
    props.setProperty("java.home", javaHome.toString());
    props.setProperty("com.sun.management.jmxremote.ssl", "false");
    props.setProperty("com.sun.management.jmxremote.local.only", "false");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();

    props.setProperty("com.sun.management.jmxremote.port", "7095");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).contains("7095");
  }

  @Test
  void authenticationDisabledWithoutARemotePortIsNotReported() {
    // Without a port the JVM starts no remote connector: nothing listens on the network.
    final Properties props = jvm();
    props.setProperty("com.sun.management.jmxremote", "true");
    props.setProperty("com.sun.management.jmxremote.authenticate", "false");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();

    props.setProperty("com.sun.management.jmxremote.port", " ");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();
  }

  @Test
  void configurationFileIsRead() throws IOException {
    final Path file = tempDir.resolve("jmx.properties");
    Files.writeString(file, """
        com.sun.management.jmxremote.port=7091
        com.sun.management.jmxremote.authenticate=false
        """);

    final Properties props = jvm();
    props.setProperty("com.sun.management.config.file", file.toString());

    final String warning = RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props);
    assertThat(warning).isNotNull();
    assertThat(warning).contains("7091");
  }

  @Test
  void commandLineOverridesTheConfigurationFile() throws IOException {
    // The JDK agent applies the -D settings over the file's, so a file that disables authentication is overruled.
    final Path file = tempDir.resolve("jmx.properties");
    Files.writeString(file, """
        com.sun.management.jmxremote.port=7091
        com.sun.management.jmxremote.authenticate=false
        """);

    final Properties props = jvm();
    props.setProperty("com.sun.management.config.file", file.toString());
    props.setProperty("com.sun.management.jmxremote.authenticate", "true");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();

    // And the other way around: the file asks for authentication, the command line disables it.
    Files.writeString(file, """
        com.sun.management.jmxremote.port=7091
        com.sun.management.jmxremote.authenticate=true
        """);
    props.setProperty("com.sun.management.jmxremote.authenticate", "false");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).contains("7091");
  }

  @Test
  void jdkDefaultManagementPropertiesIsReadWhenNoFileIsNamed() throws IOException {
    final Path javaHome = tempDir.resolve("jdk");
    final Path managementDir = javaHome.resolve("conf").resolve("management");
    Files.createDirectories(managementDir);
    Files.writeString(managementDir.resolve("management.properties"), """
        com.sun.management.jmxremote.port=7092
        com.sun.management.jmxremote.authenticate=false
        """);

    final Properties props = new Properties();
    props.setProperty("java.home", javaHome.toString());
    // The JVM starts its management agent at boot only when some -Dcom.sun.management.* setting is given: the JDK file
    // alone opens no port.
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();

    props.setProperty("com.sun.management.jmxremote", "true");
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).contains("7092");

    // A named file replaces the JDK default, it is not merged with it.
    final Path named = tempDir.resolve("named.properties");
    Files.writeString(named, "com.sun.management.jmxremote.port=7093\n");
    props.setProperty("com.sun.management.config.file", named.toString());
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();
  }

  @Test
  void unreadableConfigurationFileIsNotAnError() {
    final Properties props = jvm();
    props.setProperty("com.sun.management.config.file", tempDir.resolve("missing.properties").toString());
    assertThat(RemoteJmxCheck.unauthenticatedRemoteJmxWarning(props)).isNull();
  }

  /**
   * A JVM with no management settings: {@code java.home} points to an empty directory, so the JDK's own
   * {@code management.properties} (whatever the machine running the test has in it) does not take part.
   */
  private Properties jvm() {
    final Properties props = new Properties();
    props.setProperty("java.home", tempDir.resolve("empty-jdk").toString());
    return props;
  }
}
