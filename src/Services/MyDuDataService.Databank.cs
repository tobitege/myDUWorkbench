using Npgsql;
using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.Services;

public sealed partial class MyDuDataService
{
    public async Task WriteDatabankPropertyAsync(
        Models.DataConnectionOptions options,
        ulong elementId,
        string content,
        Func<Models.LuaPropertyRawRecord, CancellationToken, Task>? beforeWriteAsync,
        CancellationToken cancellationToken)
    {
        if (elementId == 0UL)
        {
            throw new ArgumentOutOfRangeException(nameof(elementId), "Element id must be > 0.");
        }

        await using var connection = new NpgsqlConnection(BuildConnectionString(options));
        await connection.OpenAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string selectSql = """
            SELECT property_type, value
            FROM element_property
            WHERE element_id = @elementId
              AND name = 'databank'
            FOR UPDATE;
            """;

        int propertyType;
        byte[] currentValue;
        await using (var selectCommand = new NpgsqlCommand(selectSql, connection, transaction))
        {
            selectCommand.Parameters.AddWithValue("elementId", (long)elementId);

            await using NpgsqlDataReader reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException($"Databank property for element {elementId} was not found.");
            }

            propertyType = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            currentValue = reader.IsDBNull(1) ? Array.Empty<byte>() : (byte[])reader.GetValue(1);
            if (await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException($"Duplicate databank rows found for element {elementId}.");
            }
        }

        var currentRawRecord = new Models.LuaPropertyRawRecord(
            elementId,
            "databank",
            propertyType,
            currentValue);

        if (beforeWriteAsync is not null)
        {
            await beforeWriteAsync(currentRawRecord, cancellationToken);
        }

        byte[] updatedValue = Encoding.UTF8.GetBytes(content ?? string.Empty);

        const string updateSql = """
            UPDATE element_property
            SET value = @value,
                property_type = @propertyType
            WHERE element_id = @elementId
              AND name = 'databank';
            """;

        await using var updateCommand = new NpgsqlCommand(updateSql, connection, transaction);
        updateCommand.Parameters.AddWithValue("value", updatedValue);
        updateCommand.Parameters.AddWithValue("propertyType", propertyType);
        updateCommand.Parameters.AddWithValue("elementId", (long)elementId);

        int affected = await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        if (affected != 1)
        {
            throw new InvalidOperationException($"Expected exactly one updated databank row, got {affected}.");
        }

        await transaction.CommitAsync(cancellationToken);
    }
}
