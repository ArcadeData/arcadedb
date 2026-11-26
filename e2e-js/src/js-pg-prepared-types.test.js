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

// Issue #11838: over the Postgres wire protocol a prepared statement described the columns of schemaless properties
// as varchar (text[] for a list), so a client reading through a server-prepared statement got true as "t", 42 as "42",
// 2.5 as "2.5" and [1, 2, 3] as ["1", "2", "3"], where the same statement unprepared returned typed values. A uuid
// parameter (OID 2950) could not be bound at all.
//
// node-postgres prepares on the server whenever a query config carries a `name`, from the FIRST execution: Parse once
// per connection, then Bind + Describe('P') + Execute on every execution. It has no threshold, so 8 executions per
// value stand in for the 6+ executions psycopg and pgjdbc need before they prepare.
const { Client, types } = require("pg");
const { GenericContainer, Wait } = require("testcontainers");

const ROOT_PASSWORD = "playwithdata";
const IMAGE = process.env.ARCADEDB_DOCKER_IMAGE || "arcadedata/arcadedb:latest";
const DATABASE = "pgprepared11838";
const TYPE = "T11838";
const QUERY = `SELECT v FROM ${TYPE} WHERE name = $1`;
const ID = "0f8fad5b-d9cb-469f-a165-70867728950e";
const ROUNDS = 8;
const UUID_OID = 2950;

// name -> [expected value, PostgreSQL type OIDs the column may be described with]
const EXPECTED = {
  bool: [true, [16]],
  int: [42, [23]],
  double: [2.5, [701]],
  // int4[] or int8[]: ArcadeDB describes the list the HTTP API stored as int8[], as the unnamed statement does
  list: [[1, 2, 3], [1007, 1016]],
};

// node-postgres reads int8 as a string by default, to keep a bigint's precision; these values are small, so an int8[]
// is read as numbers like an int4[]. A text[] (1009), the description issue #11838 is about, still reads as strings.
const INT4_ARRAY_OID = 1007;
const INT8_ARRAY_OID = 1016;
const typeParsers = {
  getTypeParser: (oid, format) => types.getTypeParser(oid === INT8_ARRAY_OID ? INT4_ARRAY_OID : oid, format),
};

// node-postgres passes a query's `types` both as the Parse message's parameter type OIDs and as the row type parser,
// so an array of OIDs must also answer getTypeParser
function parameterTypes(...oids) {
  return Object.assign(oids, typeParsers);
}

function assertNative(name, execution, result) {
  const [expectedValue, expectedOids] = EXPECTED[name];
  const where = `${name} execution ${execution}`;
  expect({ where, rows: result.rows.length }).toEqual({ where, rows: 1 });
  const oid = result.fields[0].dataTypeID;
  expect({ where, oid }).toEqual({ where, oid: expectedOids.includes(oid) ? oid : expectedOids });
  // toStrictEqual tells true from "t" and 42 from "42", element by element for the list
  expect({ where, value: result.rows[0].v }).toStrictEqual({ where, value: expectedValue });
}

describe("Issue #11838: prepared statements over schemaless properties (pg client)", () => {
  jest.setTimeout(120000);

  let container;
  let client;

  beforeAll(async () => {
    container = await new GenericContainer(IMAGE)
      .withExposedPorts(2480, 5432)
      .withEnvironment({
        JAVA_OPTS: `-Darcadedb.server.rootPassword=${ROOT_PASSWORD} -Darcadedb.server.plugins=PostgresProtocolPlugin`,
      })
      .withStartupTimeout(120000)
      .withWaitStrategy(Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching((statusCode) => statusCode === 204))
      .start();

    const httpBase = `http://${container.getHost()}:${container.getMappedPort(2480)}`;
    const authorization = "Basic " + Buffer.from(`root:${ROOT_PASSWORD}`).toString("base64");
    const post = async (path, body) => {
      const response = await fetch(httpBase + path, {
        method: "POST",
        headers: { Authorization: authorization, "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) throw new Error(`POST ${path} -> ${response.status}: ${await response.text()}`);
    };
    await post("/api/v1/server", { command: `create database ${DATABASE}` });
    await post(`/api/v1/command/${DATABASE}`, { language: "sql", command: `CREATE DOCUMENT TYPE ${TYPE}` });
    // no property is declared: every column read below is schemaless
    await post(`/api/v1/command/${DATABASE}`, {
      language: "sql",
      command: `INSERT INTO ${TYPE} CONTENT :r`,
      params: {
        r: [
          { name: "bool", v: true },
          { name: "int", v: 42 },
          { name: "double", v: 2.5 },
          { name: "list", v: [1, 2, 3] },
          { name: "uuid", u: ID },
        ],
      },
    });

    client = new Client({
      connectionString: `postgres://root:${ROOT_PASSWORD}@${container.getHost()}:${container.getMappedPort(5432)}/${DATABASE}`,
      types: typeParsers,
    });
    await client.connect();
  });

  afterAll(async () => {
    if (client) await client.end();
    if (container) await container.stop();
  });

  it("control: an unnamed statement returns native types", async () => {
    for (const name of Object.keys(EXPECTED))
      for (let execution = 1; execution <= ROUNDS; execution++)
        assertNative(name, execution, await client.query(QUERY, [name]));
  });

  it("a named statement returns native types on every execution", async () => {
    for (const name of Object.keys(EXPECTED))
      for (let execution = 1; execution <= ROUNDS; execution++)
        assertNative(name, execution, await client.query({ name: `s11838_${name}`, text: QUERY, values: [name] }));
  });

  it("one named statement whose rows change type between executions", async () => {
    for (let execution = 1; execution <= ROUNDS; execution++)
      for (const name of Object.keys(EXPECTED))
        assertNative(name, execution, await client.query({ name: "s11838_mixed", text: QUERY, values: [name] }));
  });

  it("binds a parameter declared as uuid", async () => {
    // node-postgres sends every parameter as text; declaring the type makes the Parse carry OID 2950, as psycopg does for
    // a uuid.UUID. PostgreSQL also reads a uuid in upper case, between braces and without hyphens.
    const spellings = [ID, ID.toUpperCase(), `{${ID}}`, ID.replace(/-/g, "")];
    for (let execution = 1; execution <= ROUNDS; execution++)
      for (const [index, spelling] of spellings.entries()) {
        const result = await client.query({
          name: `u11838_${index}`,
          text: `SELECT name, u FROM ${TYPE} WHERE u = $1`,
          values: [spelling],
          types: parameterTypes(UUID_OID),
        });
        expect({ execution, spelling, rows: result.rows }).toEqual({ execution, spelling, rows: [{ name: "uuid", u: ID }] });
      }
  });

  it("round-trips a uuid parameter through an insert", async () => {
    const written = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    await client.query({
      text: `INSERT INTO ${TYPE} SET name = 'uuid-written', u = $1`,
      values: [written.toUpperCase()],
      types: parameterTypes(UUID_OID),
    });
    for (let execution = 1; execution <= ROUNDS; execution++) {
      const result = await client.query({
        name: "u11838_read",
        text: `SELECT u FROM ${TYPE} WHERE name = 'uuid-written' AND u = $1`,
        values: [written],
        types: parameterTypes(UUID_OID),
      });
      // stored as the canonical text PostgreSQL prints, whatever spelling was bound
      expect({ execution, rows: result.rows }).toEqual({ execution, rows: [{ u: written }] });
    }
  });
});
