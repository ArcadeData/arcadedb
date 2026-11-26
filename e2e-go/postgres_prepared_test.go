//
// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
// SPDX-License-Identifier: Apache-2.0
//

package e2e_go

// Issue #11838: over the Postgres wire protocol a prepared statement described the
// columns of schemaless properties as varchar (text[] for a list), so a client
// reading through a server-prepared statement got true as "t", 42 as "42", 2.5 as
// "2.5" and [1, 2, 3] as ["1", "2", "3"], where the same statement unprepared
// returned typed values. A uuid parameter (OID 2950) could not be bound at all.
//
// pgx prepares on the server from the FIRST execution in its default mode
// (QueryExecModeCacheStatement): Parse + Describe('S') once, then Bind + Execute,
// with no Describe of the portal while the statement description has fields. Its
// low-level pgconn.ExecPrepared, like libpq's PQexecPrepared, sends Bind +
// Describe('P') + Execute on every execution. Both paths are covered below.

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	pgPort        = "5432"
	pg11838DB     = "pgprepared11838"
	pg11838Type   = "T11838"
	pg11838Query  = "SELECT v FROM " + pg11838Type + " WHERE name = $1"
	pg11838UUID   = "0f8fad5b-d9cb-469f-a165-70867728950e"
	pg11838Rounds = 8 // past every driver's prepare threshold (psycopg 6th, pgjdbc 5th, pgx 1st)

	oidBool      = 16
	oidInt4      = 23
	oidFloat8    = 701
	oidInt4Array = 1007
	oidInt8Array = 1016
	oidUUID      = 2950
)

var pg11838Names = []string{"bool", "int", "double", "list"}

// startPostgresArcade boots an ArcadeDB container with the Postgres plugin on its
// own (the shared plainContainer only enables Bolt), seeds the schemaless type the
// tests read, and terminates it when the test ends. Ports are the container's
// mapped ones.
func startPostgresArcade(t *testing.T) *pgx.ConnConfig {
	t.Helper()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        imageName(),
			ExposedPorts: []string{httpPort + "/tcp", pgPort + "/tcp"},
			Env: map[string]string{"JAVA_OPTS": "-Darcadedb.server.rootPassword=" + rootPassword +
				" -Darcadedb.server.plugins=PostgresProtocolPlugin"},
			WaitingFor: wait.ForHTTP("/api/v1/ready").
				WithPort(httpPort + "/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == http.StatusNoContent }).
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start ArcadeDB container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	hp, err := c.MappedPort(ctx, httpPort)
	if err != nil {
		t.Fatalf("mapped http port: %v", err)
	}
	pp, err := c.MappedPort(ctx, pgPort)
	if err != nil {
		t.Fatalf("mapped postgres port: %v", err)
	}
	arcade := &arcadeContainer{container: c, host: host, httpPort: hp.Port()}

	if err := createDatabase(arcade, pg11838DB); err != nil {
		t.Fatalf("create database: %v", err)
	}
	command := "/api/v1/command/" + pg11838DB
	for _, body := range []map[string]any{
		{"language": "sql", "command": "CREATE DOCUMENT TYPE " + pg11838Type},
		// no property is declared: every column read below is schemaless
		{"language": "sql", "command": "INSERT INTO " + pg11838Type + " CONTENT :r", "params": map[string]any{"r": []map[string]any{
			{"name": "bool", "v": true},
			{"name": "int", "v": 42},
			{"name": "double", "v": 2.5},
			{"name": "list", "v": []int{1, 2, 3}},
			{"name": "uuid", "u": pg11838UUID},
		}}},
	} {
		payload, _ := json.Marshal(body)
		if err := httpCommand(arcade, command, payload); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	config, err := pgx.ParseConfig(fmt.Sprintf("postgres://root:%s@%s:%s/%s?sslmode=disable", rootPassword, host, pp.Port(), pg11838DB))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	return config
}

func connectPg(t *testing.T, config *pgx.ConnConfig, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cfg := config.Copy()
	cfg.DefaultQueryExecMode = mode
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	return conn
}

// checkNative fails the test unless value is the native Go value pgx decodes the
// given schemaless property to, and the column was described with a type that
// carries it. It returns false on a failure, so the caller reports each property once.
func checkNative(t *testing.T, name string, execution int, oid uint32, value any) bool {
	t.Helper()
	where := fmt.Sprintf("%s execution %d (column OID %d)", name, execution, oid)
	switch name {
	case "bool":
		if oid != oidBool || value != true {
			t.Errorf("%s: got %#v (%T), want bool true", where, value, value)
			return false
		}
	case "int":
		if oid != oidInt4 || value != int32(42) {
			t.Errorf("%s: got %#v (%T), want int32 42", where, value, value)
			return false
		}
	case "double":
		if oid != oidFloat8 || value != 2.5 {
			t.Errorf("%s: got %#v (%T), want float64 2.5", where, value, value)
			return false
		}
	case "list":
		elements, ok := value.([]any)
		if !ok || len(elements) != 3 || (oid != oidInt4Array && oid != oidInt8Array) {
			t.Errorf("%s: got %#v (%T), want an int4[] or int8[] of 1, 2, 3", where, value, value)
			return false
		}
		for i, element := range elements {
			switch element.(type) {
			case int32, int64:
			default:
				t.Errorf("%s: element %d is %#v (%T), not an integer", where, i, element, element)
				return false
			}
			if fmt.Sprint(element) != fmt.Sprint(i+1) {
				t.Errorf("%s: element %d is %#v", where, i, element)
				return false
			}
		}
	}
	return true
}

func TestPostgres_Issue11838_PreparedSchemalessTypes(t *testing.T) {
	config := startPostgresArcade(t)

	// pgx's default: the statement is prepared on the server at its first execution and its
	// description, from Describe('S'), is what every later execution's rows are decoded with
	t.Run("CacheStatement", func(t *testing.T) {
		conn := connectPg(t, config, pgx.QueryExecModeCacheStatement)
		for _, name := range pg11838Names {
			for execution := 1; execution <= pg11838Rounds; execution++ {
				rows, err := conn.Query(ctx, pg11838Query, name)
				if err != nil {
					t.Fatalf("%s execution %d: %v", name, execution, err)
				}
				var values []any
				var oid uint32
				count := 0
				for rows.Next() {
					count++
					oid = rows.FieldDescriptions()[0].DataTypeOID
					if values, err = rows.Values(); err != nil {
						t.Fatalf("%s execution %d: %v", name, execution, err)
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					t.Fatalf("%s execution %d: %v", name, execution, err)
				}
				if count != 1 {
					t.Fatalf("%s execution %d: %d rows", name, execution, count)
				}
				if !checkNative(t, name, execution, oid, values[0]) {
					break
				}
			}
		}
	})

	// the libpq flow (psycopg's from its 6th execution): Parse once, then Bind + Describe('P') +
	// Execute on every execution, one named statement serving rows of a different type each time
	t.Run("ExecPreparedDescribesThePortal", func(t *testing.T) {
		conn := connectPg(t, config, pgx.QueryExecModeCacheStatement)
		if _, err := conn.PgConn().Prepare(ctx, "s11838", pg11838Query, nil); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		failed := map[string]bool{}
		for execution := 1; execution <= pg11838Rounds; execution++ {
			for _, name := range pg11838Names {
				if failed[name] {
					continue
				}
				result := conn.PgConn().ExecPrepared(ctx, "s11838", [][]byte{[]byte(name)}, nil, nil).Read()
				if result.Err != nil {
					t.Fatalf("%s execution %d: %v", name, execution, result.Err)
				}
				if len(result.Rows) != 1 || len(result.FieldDescriptions) != 1 {
					t.Fatalf("%s execution %d: %d rows, %d columns", name, execution, len(result.Rows), len(result.FieldDescriptions))
				}
				field := result.FieldDescriptions[0]
				var value any
				if err := conn.TypeMap().Scan(field.DataTypeOID, field.Format, result.Rows[0][0], &value); err != nil {
					t.Fatalf("%s execution %d: decoding %q as OID %d: %v", name, execution, result.Rows[0][0], field.DataTypeOID, err)
				}
				failed[name] = !checkNative(t, name, execution, field.DataTypeOID, value)
			}
		}
	})

	t.Run("UuidParameter", func(t *testing.T) {
		id, err := hex.DecodeString(strings.ReplaceAll(pg11838UUID, "-", ""))
		if err != nil {
			t.Fatal(err)
		}
		var bytes16 [16]byte
		copy(bytes16[:], id)

		// pgx's own API: QueryExecModeExec encodes a pgtype.UUID as its text but declares no parameter type (OID 0), and the
		// prepared modes take the type from the server's ParameterDescription, so no high-level pgx call sends OID 2950
		conn := connectPg(t, config, pgx.QueryExecModeExec)
		for execution := 1; execution <= pg11838Rounds; execution++ {
			var name, u string
			if err := conn.QueryRow(ctx, "SELECT name, u FROM "+pg11838Type+" WHERE u = $1",
				pgtype.UUID{Bytes: bytes16, Valid: true}).Scan(&name, &u); err != nil {
				t.Fatalf("execution %d: %v", execution, err)
			}
			if name != "uuid" || u != pg11838UUID {
				t.Fatalf("execution %d: got (%q, %q)", execution, name, u)
			}
		}

		// a named statement whose parameter is declared uuid (OID 2950, as psycopg, pgjdbc and Npgsql declare one), bound
		// in text and in binary (the 16 bytes)
		if _, err := conn.PgConn().Prepare(ctx, "u11838", "SELECT name, u FROM "+pg11838Type+" WHERE u = $1", []uint32{oidUUID}); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		for execution := 1; execution <= pg11838Rounds; execution++ {
			for _, format := range []struct {
				code  int16
				value []byte
			}{{0, []byte(strings.ToUpper(pg11838UUID))}, {1, id}} {
				result := conn.PgConn().ExecPrepared(ctx, "u11838", [][]byte{format.value}, []int16{format.code}, nil).Read()
				if result.Err != nil {
					t.Fatalf("execution %d, format %d: %v", execution, format.code, result.Err)
				}
				if len(result.Rows) != 1 || string(result.Rows[0][0]) != "uuid" || string(result.Rows[0][1]) != pg11838UUID {
					t.Fatalf("execution %d, format %d: got %q", execution, format.code, result.Rows)
				}
			}
		}

		// round trip: a uuid written through a bound parameter reads back as its canonical text
		written := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
		var writtenUUID pgtype.UUID
		if err := writtenUUID.Scan(written); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, "INSERT INTO "+pg11838Type+" SET name = 'uuid-written', u = $1", writtenUUID); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var u string
		if err := conn.QueryRow(ctx, "SELECT u FROM "+pg11838Type+" WHERE name = 'uuid-written' AND u = $1", writtenUUID).Scan(&u); err != nil {
			t.Fatalf("read back: %v", err)
		}
		if u != written {
			t.Fatalf("read back %q, want %q", u, written)
		}
	})
}
