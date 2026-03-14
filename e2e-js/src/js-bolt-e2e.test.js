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
const neo4j = require("neo4j-driver");
const { GenericContainer, Wait } = require("testcontainers");

describe("E2E tests using Neo4j Bolt driver (issue #3650)", () => {
  jest.setTimeout(120000);

  let arcadedbContainer;
  let driver;
  let session;

  const DB_NAME = "testbolt";
  const PASSWORD = "playwithdata";

  async function httpApi(host, port, payload) {
    const response = await fetch(`http://${host}:${port}/api/v1/server`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Basic " + Buffer.from(`root:${PASSWORD}`).toString("base64"),
      },
      body: JSON.stringify(payload),
    });
    if (!response.ok)
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    return response.json();
  }

  beforeAll(async () => {
    arcadedbContainer = await new GenericContainer("arcadedata/arcadedb:latest")
      .withExposedPorts(2480, 7687)
      .withEnvironment({
        JAVA_OPTS:
          `-Darcadedb.server.rootPassword=${PASSWORD}` +
          " -Darcadedb.server.plugins=BoltProtocolPlugin",
      })
      .withStartupTimeout(60000)
      .withWaitStrategy(Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching((statusCode) => statusCode === 204))
      .start();

    const host = arcadedbContainer.getHost();
    const httpPort = arcadedbContainer.getMappedPort(2480);
    const boltPort = arcadedbContainer.getMappedPort(7687);

    // Create test database via HTTP API
    await httpApi(host, httpPort, { command: `create database ${DB_NAME}` });

    driver = neo4j.driver(
      `bolt://${host}:${boltPort}`,
      neo4j.auth.basic("root", PASSWORD)
    );
    session = driver.session({ database: DB_NAME });

    // Create test data
    const nodes = [
      { id: "node-1", name: "Alice", value: neo4j.int(100) },
      { id: "node-2", name: "Bob", value: neo4j.int(200) },
      { id: "node-3", name: "Charlie", value: neo4j.int(300) },
    ];
    for (const params of nodes) {
      await session.run(
        "CREATE (:TestNode {id: $id, name: $name, value: $value})",
        params
      );
    }
  });

  afterAll(async () => {
    try {
      if (session) {
        await session.run("MATCH (t:TestNode) DETACH DELETE t");
        await session.close();
      }
      if (driver) await driver.close();
      if (arcadedbContainer) {
        const host = arcadedbContainer.getHost();
        const httpPort = arcadedbContainer.getMappedPort(2480);
        await httpApi(host, httpPort, { command: `drop database ${DB_NAME}` });
        await arcadedbContainer.stop();
      }
    } catch (e) {
      console.error("Cleanup error:", e.message);
    }
  });

  it("should filter by WHERE clause with string parameter", async () => {
    const result = await session.run(
      "MATCH (t:TestNode) WHERE t.id = $id RETURN t.name AS name, t.value AS value",
      { id: "node-1" }
    );
    expect(result.records).toHaveLength(1);
    expect(result.records[0].get("name")).toBe("Alice");
    expect(result.records[0].get("value").toNumber()).toBe(100);
  });

  it("should filter by WHERE clause with a different string value", async () => {
    const result = await session.run(
      "MATCH (t:TestNode) WHERE t.id = $id RETURN t.name AS name",
      { id: "node-2" }
    );
    expect(result.records).toHaveLength(1);
    expect(result.records[0].get("name")).toBe("Bob");
  });

  it("should return 0 results for non-matching parameter", async () => {
    const result = await session.run(
      "MATCH (t:TestNode) WHERE t.id = $id RETURN t.name AS name",
      { id: "nonexistent" }
    );
    expect(result.records).toHaveLength(0);
  });

  it("should filter by WHERE clause with AND and multiple parameters", async () => {
    const result = await session.run(
      "MATCH (t:TestNode) WHERE t.id = $id AND t.value = $val RETURN t.name AS name",
      { id: "node-1", val: neo4j.int(100) }
    );
    expect(result.records).toHaveLength(1);
    expect(result.records[0].get("name")).toBe("Alice");
  });

  it("should filter by WHERE clause with integer parameter", async () => {
    const result = await session.run(
      "MATCH (t:TestNode) WHERE t.value = $val RETURN t.name AS name",
      { val: neo4j.int(200) }
    );
    expect(result.records).toHaveLength(1);
    expect(result.records[0].get("name")).toBe("Bob");
  });

  it("should filter by property map syntax as baseline", async () => {
    const result = await session.run(
      "MATCH (t:TestNode {id: $id}) RETURN t.name AS name",
      { id: "node-1" }
    );
    expect(result.records).toHaveLength(1);
    expect(result.records[0].get("name")).toBe("Alice");
  });
});
