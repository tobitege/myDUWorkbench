using myDUWorkbench.Models;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.Services;

public sealed partial class MyDuDataService
{
    private static bool LooksLikeJson(IReadOnlyList<byte> payload)
    {
        if (payload.Count == 0)
        {
            return false;
        }

        byte first = payload[0];
        return first == (byte)'{' || first == (byte)'[';
    }

    private static string BuildRawPreview(byte[] payload)
    {
        if (payload.Length == 0)
        {
            return "<empty>";
        }

        if (LooksLikeJson(payload))
        {
            string text = Encoding.UTF8.GetString(payload);
            return text.Length <= 4000 ? text : text[..4000];
        }

        string utf8 = Encoding.UTF8.GetString(payload);
        if (IsMostlyPrintable(utf8))
        {
            return utf8.Length <= 4000 ? utf8 : utf8[..4000];
        }

        int count = Math.Min(128, payload.Length);
        byte[] prefix = payload.Take(count).ToArray();
        return $"hex:{Convert.ToHexString(prefix)}";
    }

    private static string BuildConnectionString(DataConnectionOptions options)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = options.Host,
            Port = options.Port,
            Database = options.Database,
            Username = options.Username,
            Password = options.Password,
            Timeout = 5,
            CommandTimeout = 15
        };

        return builder.ConnectionString;
    }

    private static string BuildSqlLikePattern(string searchInput)
    {
        string trimmed = searchInput.Trim();
        string normalized = trimmed.Replace('*', '%').Replace('?', '_');

        if (trimmed.IndexOf('%') >= 0 || trimmed.IndexOf('*') >= 0)
        {
            return normalized;
        }

        if (normalized.IndexOf('%') < 0 && normalized.IndexOf('_') < 0)
        {
            normalized = "%" + normalized + "%";
        }

        return normalized;
    }

    private static string[] BuildCoreKindFilter(IReadOnlyCollection<ConstructCoreKind>? coreKinds)
    {
        if (coreKinds is null || coreKinds.Count == 0)
        {
            return DefaultCoreKindFilter;
        }

        var filter = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ConstructCoreKind coreKind in coreKinds)
        {
            switch (coreKind)
            {
                case ConstructCoreKind.Dynamic:
                    filter.Add("dynamic");
                    break;
                case ConstructCoreKind.Static:
                    filter.Add("static");
                    break;
                case ConstructCoreKind.Space:
                    filter.Add("space");
                    break;
                case ConstructCoreKind.Unknown:
                    filter.Add("unknown");
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(coreKinds), coreKind, "Unsupported construct core kind filter.");
            }
        }

        if (filter.Count == 0)
        {
            return DefaultCoreKindFilter;
        }

        return filter.ToArray();
    }

    private static string BuildConstructOrderClause(ConstructListSort sortBy, bool descending)
    {
        string direction = descending ? "DESC" : "ASC";
        return sortBy switch
        {
            ConstructListSort.Name => $"c.name {direction} NULLS LAST, c.id {direction}",
            ConstructListSort.Id => $"c.id {direction}, c.name {direction} NULLS LAST",
            _ => throw new ArgumentOutOfRangeException(nameof(sortBy), sortBy, "Unsupported construct sort field.")
        };
    }

    private static ConstructCoreKind ParseCoreKind(string rawCoreKind)
    {
        if (string.Equals(rawCoreKind, "dynamic", StringComparison.OrdinalIgnoreCase))
        {
            return ConstructCoreKind.Dynamic;
        }

        if (string.Equals(rawCoreKind, "static", StringComparison.OrdinalIgnoreCase))
        {
            return ConstructCoreKind.Static;
        }

        if (string.Equals(rawCoreKind, "space", StringComparison.OrdinalIgnoreCase))
        {
            return ConstructCoreKind.Space;
        }

        return ConstructCoreKind.Unknown;
    }

    private static async Task<bool> TableExistsAsync(
        NpgsqlConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT to_regclass(@tableName) IS NOT NULL;";
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("tableName", tableName);

        object? result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is true;
    }

    private static ulong? TryGetUInt64(NpgsqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        object value = reader.GetValue(ordinal);
        return value switch
        {
            ulong u => u,
            long l when l >= 0L => (ulong)l,
            int i when i >= 0 => (ulong)i,
            decimal d when d >= 0 => (ulong)d,
            _ => ulong.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsed)
                ? parsed
                : null
        };
    }

    private static double ReadDouble(NpgsqlDataReader reader, int ordinal)
    {
        object value = reader.GetValue(ordinal);
        return Convert.ToDouble(value, CultureInfo.InvariantCulture);
    }

    private static float ReadSingle(NpgsqlDataReader reader, int ordinal)
    {
        object value = reader.GetValue(ordinal);
        return Convert.ToSingle(value, CultureInfo.InvariantCulture);
    }
}
