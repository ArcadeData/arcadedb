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
package com.arcadedb.server.gremlin;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.util.*;

/**
 * Extends the default Gremlin Setting class to support a more recent version of snakeyaml because of https://nvd.nist.gov/vuln/detail/CVE-2022-1471
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBGremlinSettings extends Settings {
  /**
   * Creates {@link Constructor} which contains all configurations to parse
   * a Gremlin Server YAML configuration file using SnakeYAML.
   *
   * @return a {@link Constructor} to parse a Gremlin Server YAML
   */
  protected static Constructor createDefaultYamlConstructor() {
    LoaderOptions options = new LoaderOptions();
    final Constructor constructor = new Constructor(Settings.class, options);
    final TypeDescription settingsDescription = new TypeDescription(Settings.class);
    settingsDescription.addPropertyParameters("graphs", String.class, String.class);
    settingsDescription.addPropertyParameters("scriptEngines", String.class, ScriptEngineSettings.class);
    settingsDescription.addPropertyParameters("serializers", SerializerSettings.class);
    settingsDescription.addPropertyParameters("plugins", String.class);
    settingsDescription.addPropertyParameters("processors", ProcessorSettings.class);
    constructor.addTypeDescription(settingsDescription);

    final TypeDescription serializerSettingsDescription = new TypeDescription(SerializerSettings.class);
    serializerSettingsDescription.putMapPropertyType("config", String.class, Object.class);
    constructor.addTypeDescription(serializerSettingsDescription);

    final TypeDescription scriptEngineSettingsDescription = new TypeDescription(ScriptEngineSettings.class);
    scriptEngineSettingsDescription.addPropertyParameters("imports", String.class);
    scriptEngineSettingsDescription.addPropertyParameters("staticImports", String.class);
    scriptEngineSettingsDescription.addPropertyParameters("scripts", String.class);
    scriptEngineSettingsDescription.addPropertyParameters("config", String.class, Object.class);
    scriptEngineSettingsDescription.addPropertyParameters("plugins", String.class, Object.class);
    constructor.addTypeDescription(scriptEngineSettingsDescription);

    final TypeDescription sslSettings = new TypeDescription(SslSettings.class);
    constructor.addTypeDescription(sslSettings);

    final TypeDescription authenticationSettings = new TypeDescription(AuthenticationSettings.class);
    constructor.addTypeDescription(authenticationSettings);

    final TypeDescription serverMetricsDescription = new TypeDescription(ServerMetrics.class);
    constructor.addTypeDescription(serverMetricsDescription);

    final TypeDescription consoleReporterDescription = new TypeDescription(ConsoleReporterMetrics.class);
    constructor.addTypeDescription(consoleReporterDescription);

    final TypeDescription csvReporterDescription = new TypeDescription(CsvReporterMetrics.class);
    constructor.addTypeDescription(csvReporterDescription);

    final TypeDescription jmxReporterDescription = new TypeDescription(JmxReporterMetrics.class);
    constructor.addTypeDescription(jmxReporterDescription);

    final TypeDescription slf4jReporterDescription = new TypeDescription(Slf4jReporterMetrics.class);
    constructor.addTypeDescription(slf4jReporterDescription);

    final TypeDescription gangliaReporterDescription = new TypeDescription(GangliaReporterMetrics.class);
    constructor.addTypeDescription(gangliaReporterDescription);

    final TypeDescription graphiteReporterDescription = new TypeDescription(GraphiteReporterMetrics.class);
    constructor.addTypeDescription(graphiteReporterDescription);
    return constructor;
  }

  public static Settings read(final InputStream stream) {
    Objects.requireNonNull(stream);

    final Constructor constructor = createDefaultYamlConstructor();
    final Yaml yaml = new Yaml(constructor);
    return yaml.loadAs(stream, Settings.class);
  }
}
