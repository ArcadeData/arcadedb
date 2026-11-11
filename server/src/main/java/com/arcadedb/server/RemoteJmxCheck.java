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

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;

/**
 * Tells whether the JVM hosting the server runs a remote JMX agent that accepts connections without authentication. A
 * client of such an agent can invoke any MBean of the platform server, which amounts to running code inside the server
 * process and reading every byte of its heap, so the server reports it at startup however the agent was configured.
 * <p>
 * The settings are resolved the way the JDK management agent resolves them: the file named by
 * {@code com.sun.management.config.file}, or the JDK's own {@code conf/management/management.properties} when none is
 * named, overridden by the {@code com.sun.management.*} system properties. The agent is started at boot only when one of
 * the system properties {@code com.sun.management.config.file}, {@code com.sun.management.jmxremote} or
 * {@code com.sun.management.jmxremote.port} is given ({@code jdk.internal.agent.Agent#getManagementProperties}), and opens
 * a network port only when a remote port is set.
 * <p>
 * The check runs once, at server startup, on the boot-time configuration. An agent an operator starts later on the running
 * process with {@code jcmd <pid> ManagementAgent.start} takes its settings from the command arguments and is not covered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RemoteJmxCheck {
  static final String MANAGEMENT_PREFIX = "com.sun.management.";
  static final String CONFIG_FILE       = "com.sun.management.config.file";
  static final String JMXREMOTE         = "com.sun.management.jmxremote";
  static final String PORT              = "com.sun.management.jmxremote.port";
  static final String AUTHENTICATE      = "com.sun.management.jmxremote.authenticate";

  private RemoteJmxCheck() {
  }

  /**
   * Returns the warning to log when the JVM described by {@code systemProperties} listens for remote JMX connections
   * without authentication, or {@code null} when it does not.
   */
  static String unauthenticatedRemoteJmxWarning(final Properties systemProperties) {
    if (systemProperties.getProperty(CONFIG_FILE) == null && systemProperties.getProperty(JMXREMOTE) == null
        && systemProperties.getProperty(PORT) == null)
      return null;

    final Properties effective = new Properties();
    final String configFile = systemProperties.getProperty(CONFIG_FILE);
    if (configFile != null)
      load(Path.of(configFile), effective);
    else {
      final String javaHome = systemProperties.getProperty("java.home");
      if (javaHome != null)
        load(Path.of(javaHome, "conf", "management", "management.properties"), effective);
    }

    for (final String name : systemProperties.stringPropertyNames())
      if (name.startsWith(MANAGEMENT_PREFIX))
        effective.setProperty(name, systemProperties.getProperty(name));

    final String port = effective.getProperty(PORT);
    if (port == null || port.isBlank())
      return null;

    // The JDK reads the flag with Boolean.valueOf, untrimmed: authentication is on when the setting is absent, and off for
    // any value other than "true" in any case, "true " with a trailing blank included.
    final String authenticate = effective.getProperty(AUTHENTICATE);
    if (authenticate == null || Boolean.parseBoolean(authenticate))
      return null;

    return String.format(Locale.ROOT, """
        The JVM accepts remote JMX connections on port %s without authentication (com.sun.management.jmxremote.authenticate=%s). \
        Anyone who can open a connection to that port can run code inside this server and read its memory, credentials \
        included. Remove the com.sun.management.jmxremote.* settings from ARCADEDB_JMX, JAVA_OPTS and JAVA_TOOL_OPTIONS (or the \
        JMX configuration file) to switch remote JMX off \
        (local tools such as jconsole and jcmd attach without it), or enable authentication and SSL""", port.trim(),
        authenticate);
  }

  private static void load(final Path file, final Properties target) {
    if (!Files.isRegularFile(file))
      return;
    try (final InputStream in = Files.newInputStream(file)) {
      target.load(in);
    } catch (final IOException | IllegalArgumentException e) {
      // AN UNREADABLE FILE STOPS THE JVM AGENT FROM STARTING AT ALL, SO IT OPENS NO PORT EITHER
      LogManager.instance().log(RemoteJmxCheck.class, Level.FINE, "Cannot read the JMX configuration file '%s'", e, file);
    }
  }
}
