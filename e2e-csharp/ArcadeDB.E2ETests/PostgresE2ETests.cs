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

using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace ArcadeDB.E2ETests;

[Collection("ArcadeDB")]
public class PostgresE2ETests
{
    private readonly ArcadeDbFixture _fixture;

    public PostgresE2ETests(ArcadeDbFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task BasicConnection()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
    }

    [Fact]
    public async Task SimpleQuery()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT FROM schema:types";
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(reader.HasRows);
    }

    [Fact]
    public async Task CreateTypeAndInsert()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();

        cmd.CommandText = "INSERT INTO NpgsqlTest SET id = 'ci1', name = 'Alice', value = '100'";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO NpgsqlTest SET id = 'ci2', name = 'Bob', value = '200'";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "SELECT FROM NpgsqlTest WHERE id IN ['ci1', 'ci2']";
        await using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        while (await reader.ReadAsync()) count++;
        Assert.True(count >= 2);
    }

    [Fact]
    public async Task ParameterizedSelect()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO NpgsqlTest SET id = 'ps1', name = 'Alice', value = '100'";
        await insert.ExecuteNonQueryAsync();

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT FROM NpgsqlTest WHERE id = $1";
        select.Parameters.AddWithValue("ps1");
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("Alice", reader.GetString(reader.GetOrdinal("name")));
    }

    [Fact]
    public async Task MultipleParameters()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO NpgsqlTest SET id = 'mp1', name = 'Alice', value = '100'";
        await insert.ExecuteNonQueryAsync();

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT FROM NpgsqlTest WHERE name = $1 AND value = $2";
        select.Parameters.AddWithValue("Alice");
        select.Parameters.AddWithValue("100");
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
    }

    [Fact]
    public async Task ParameterizedInsert()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO NpgsqlTest SET id = $1, name = $2, value = $3";
        insert.Parameters.AddWithValue("pi1");
        insert.Parameters.AddWithValue("Charlie");
        insert.Parameters.AddWithValue("300");

        try
        {
            await insert.ExecuteNonQueryAsync();
        }
        catch (NpgsqlException ex) when (string.IsNullOrEmpty(ex.SqlState))
        {
            // ArcadeDB sends a RowDescription after INSERT which Npgsql rejects (protocol deviation); SqlState is null for protocol errors so SQL errors propagate normally.
        }

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT FROM NpgsqlTest WHERE id = $1";
        select.Parameters.AddWithValue("pi1");
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("Charlie", reader.GetString(reader.GetOrdinal("name")));
    }

    [Fact]
    public async Task TextOidParameterBinding()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();
        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO NpgsqlTest SET id = 'tob1', name = 'OidTextUser', value = '77'";
        await insert.ExecuteNonQueryAsync();

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT FROM NpgsqlTest WHERE name = $1";
        // NpgsqlDbType.Text forces OID 25 in the bind message; PG JDBC sends varchar (OID 1043) instead
        select.Parameters.Add(new NpgsqlParameter { Value = "OidTextUser", NpgsqlDbType = NpgsqlDbType.Text });
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("OidTextUser", reader.GetString(reader.GetOrdinal("name")));
    }

    [Fact]
    public async Task Transaction()
    {
        await using var conn = await _fixture.DataSource.OpenConnectionAsync();

        // ArcadeDB only accepts bare BEGIN; Npgsql's BeginTransactionAsync sends
        // BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED which ArcadeDB rejects.
        await using var begin = conn.CreateCommand();
        begin.CommandText = "BEGIN";
        await begin.ExecuteNonQueryAsync();

        await using var insert = conn.CreateCommand();
        insert.CommandText = "INSERT INTO NpgsqlTest SET id = 'tx1', name = 'TxTest', value = '999'";
        await insert.ExecuteNonQueryAsync();

        await using var commit = conn.CreateCommand();
        commit.CommandText = "COMMIT";
        await commit.ExecuteNonQueryAsync();

        await using var select = conn.CreateCommand();
        select.CommandText = "SELECT FROM NpgsqlTest WHERE id = $1";
        select.Parameters.AddWithValue("tx1");
        await using var reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("TxTest", reader.GetString(reader.GetOrdinal("name")));
    }
}
