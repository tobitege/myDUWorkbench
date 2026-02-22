using K4os.Compression.LZ4;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;

namespace myDUWorkbench.Services;

public sealed record VoxelMaterialEntry(
    string MaterialId,
    string MaterialName,
    long VoxelBlocks,
    double VolumeLiters);

public sealed record VoxelMaterialSummary(
    int ChunkCount,
    int ParsedChunkCount,
    int FailedChunkCount,
    long TotalVoxelBlocks,
    double TotalVolumeLiters,
    IReadOnlyList<VoxelMaterialEntry> Materials,
    IReadOnlyList<string> Warnings);

public static class BlueprintVoxelMaterialDecoder
{
    private const uint MagicLz4 = 0xFB14B6F9;
    private const uint MagicZlib = 0x124F0359;
    private const uint MagicUncompressed = 0x8C488FE9;

    private const uint MagicVoxelCellData = 0x27B8A013;
    private const uint MagicVertexGrid = 0xE881339E;

    private const double LitersPerVoxelBlock = 15.625d;
    private const int WarningLimit = 25;

    public static VoxelMaterialSummary Summarize(JsonArray cells)
    {
        if (cells is null)
        {
            throw new ArgumentNullException(nameof(cells));
        }

        var globalCounts = new Dictionary<MaterialAggregateKey, long>();
        var warnings = new List<string>();

        long totalVoxelBlocks = 0L;
        int parsedChunks = 0;
        int failedChunks = 0;

        foreach (JsonNode? node in cells)
        {
            if (node is not JsonObject chunkObject)
            {
                failedChunks++;
                AddWarning(warnings, "Error parsing chunk (unknown coords): chunk is not a JSON object.");
                continue;
            }

            string coordsText = BuildChunkCoordsText(chunkObject);
            try
            {
                JsonNode voxelBinaryNode = ResolveVoxelBinaryNode(chunkObject);
                byte[] raw = DecodeBase64Field(voxelBinaryNode);
                byte[] decoded = DecompressNq(raw);

                ChunkMaterialTotals chunkTotals = ParseVoxelCellData(decoded);
                foreach ((MaterialAggregateKey key, long count) in chunkTotals.CountsByMaterial)
                {
                    globalCounts[key] = globalCounts.TryGetValue(key, out long current)
                        ? current + count
                        : count;
                    totalVoxelBlocks += count;
                }

                parsedChunks++;
            }
            catch (Exception ex) when (ex is VoxelDeserializeException || ex is InvalidOperationException)
            {
                failedChunks++;
                AddWarning(warnings, $"Error parsing chunk at {coordsText}: {ex.Message}");
            }
        }

        VoxelMaterialEntry[] materials = globalCounts
            .Select(entry => new VoxelMaterialEntry(
                entry.Key.MaterialId,
                entry.Key.MaterialName,
                entry.Value,
                entry.Value * LitersPerVoxelBlock))
            .OrderByDescending(entry => entry.VoxelBlocks)
            .ThenBy(entry => entry.MaterialName, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return new VoxelMaterialSummary(
            cells.Count,
            parsedChunks,
            failedChunks,
            totalVoxelBlocks,
            totalVoxelBlocks * LitersPerVoxelBlock,
            materials,
            warnings);
    }

    private static void AddWarning(ICollection<string> warnings, string warning)
    {
        if (warnings.Count < WarningLimit)
        {
            warnings.Add(warning);
        }
    }

    private static string BuildChunkCoordsText(JsonObject chunkObject)
    {
        if (TryParseInt64Field(chunkObject["x"], out long x) &&
            TryParseInt64Field(chunkObject["y"], out long y) &&
            TryParseInt64Field(chunkObject["z"], out long z))
        {
            return $"({x}, {y}, {z})";
        }

        return "(unknown coords)";
    }

    private static JsonNode ResolveVoxelBinaryNode(JsonObject chunkObject)
    {
        if (!TryGetPropertyIgnoreCase(chunkObject, "records", out JsonNode? recordsNode) ||
            recordsNode is not JsonObject recordsObject)
        {
            throw new VoxelDeserializeException("Missing records object.");
        }

        if (!TryGetPropertyIgnoreCase(recordsObject, "voxel", out JsonNode? voxelNode) ||
            voxelNode is not JsonObject voxelObject)
        {
            throw new VoxelDeserializeException("Missing records.voxel object.");
        }

        if (!TryGetPropertyIgnoreCase(voxelObject, "data", out JsonNode? dataNode) || dataNode is null)
        {
            throw new VoxelDeserializeException("Missing records.voxel.data node.");
        }

        if (dataNode is JsonObject dataObject &&
            TryGetPropertyIgnoreCase(dataObject, "$binary", out JsonNode? wrappedBinaryNode) &&
            wrappedBinaryNode is not null)
        {
            return wrappedBinaryNode;
        }

        return dataNode;
    }

    private static bool TryGetPropertyIgnoreCase(
        JsonObject obj,
        string propertyName,
        out JsonNode? value)
    {
        foreach ((string key, JsonNode? node) in obj)
        {
            if (string.Equals(key, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = node;
                return true;
            }
        }

        value = null;
        return false;
    }

    private static bool TryParseInt64Field(JsonNode? node, out long value)
    {
        value = 0L;

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<long>(out long asLong))
            {
                value = asLong;
                return true;
            }

            if (scalar.TryGetValue<int>(out int asInt))
            {
                value = asInt;
                return true;
            }

            if (scalar.TryGetValue<string>(out string? asString) &&
                long.TryParse(asString, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
            {
                value = parsed;
                return true;
            }
        }

        if (node is JsonObject obj &&
            TryGetPropertyIgnoreCase(obj, "$numberLong", out JsonNode? numberLongNode) &&
            numberLongNode is JsonValue numberLongValue &&
            numberLongValue.TryGetValue<string>(out string? wrapped) &&
            long.TryParse(wrapped, NumberStyles.Integer, CultureInfo.InvariantCulture, out long wrappedParsed))
        {
            value = wrappedParsed;
            return true;
        }

        return false;
    }

    private static byte[] DecodeBase64Field(JsonNode node)
    {
        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<string>(out string? encodedText) && !string.IsNullOrWhiteSpace(encodedText))
            {
                return DecodeBase64Text(encodedText);
            }

            throw new VoxelDeserializeException("Unsupported $binary scalar payload.");
        }

        if (node is JsonObject obj)
        {
            if (TryGetPropertyIgnoreCase(obj, "base64", out JsonNode? base64Node) &&
                base64Node is JsonValue base64Value &&
                base64Value.TryGetValue<string>(out string? base64Text) &&
                !string.IsNullOrWhiteSpace(base64Text))
            {
                return DecodeBase64Text(base64Text);
            }

            if (TryGetPropertyIgnoreCase(obj, "$binary", out JsonNode? nestedBinaryNode) &&
                nestedBinaryNode is not null)
            {
                return DecodeBase64Field(nestedBinaryNode);
            }

            throw new VoxelDeserializeException("Unsupported $binary object shape.");
        }

        throw new VoxelDeserializeException("Unsupported $binary node type.");
    }

    private static byte[] DecodeBase64Text(string encoded)
    {
        try
        {
            return Convert.FromBase64String(encoded.Trim());
        }
        catch (FormatException ex)
        {
            throw new VoxelDeserializeException("Invalid base64 payload.", ex);
        }
    }

    private static byte[] DecompressNq(byte[] data)
    {
        if (data.Length < 12)
        {
            return data;
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0, 4));
        ulong expectedSize = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(4, 8));
        byte[] payload = data.AsSpan(12).ToArray();

        if (magic == MagicUncompressed)
        {
            return payload;
        }

        if (magic == MagicZlib)
        {
            byte[] decompressed = DecompressZlib(payload);
            if (expectedSize > 0UL && decompressed.LongLength != (long)expectedSize)
            {
                throw new VoxelDeserializeException(
                    $"Zlib size mismatch: expected {expectedSize}, got {decompressed.LongLength}.");
            }

            return decompressed;
        }

        if (magic == MagicLz4)
        {
            return DecompressLz4(payload, expectedSize);
        }

        return data;
    }

    private static byte[] DecompressZlib(byte[] payload)
    {
        try
        {
            using var input = new MemoryStream(payload, writable: false);
            using var zlib = new ZLibStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            zlib.CopyTo(output);
            return output.ToArray();
        }
        catch (InvalidDataException ex)
        {
            throw new VoxelDeserializeException("Failed to decompress zlib payload.", ex);
        }
    }

    private static byte[] DecompressLz4(byte[] payload, ulong expectedSize)
    {
        if (expectedSize > 0UL && expectedSize <= int.MaxValue &&
            TryDecodeLz4(payload, (int)expectedSize, out byte[] decodedByExpectedSize))
        {
            return decodedByExpectedSize;
        }

        if (TryDecodeLz4WithGuessedSize(payload, out byte[] decodedByGuess))
        {
            return decodedByGuess;
        }

        throw new VoxelDeserializeException("Failed to decompress LZ4 payload.");
    }

    private static bool TryDecodeLz4(byte[] payload, int targetSize, out byte[] decoded)
    {
        decoded = Array.Empty<byte>();
        if (targetSize <= 0)
        {
            return false;
        }

        var output = new byte[targetSize];
        int decodedLength = LZ4Codec.Decode(
            payload,
            0,
            payload.Length,
            output,
            0,
            output.Length);
        if (decodedLength <= 0)
        {
            return false;
        }

        decoded = decodedLength == output.Length
            ? output
            : output.AsSpan(0, decodedLength).ToArray();
        return true;
    }

    private static bool TryDecodeLz4WithGuessedSize(byte[] payload, out byte[] decoded)
    {
        decoded = Array.Empty<byte>();

        const int maxTargetSize = 128 * 1024 * 1024;
        int guess = Math.Max(payload.Length * 4, 1024);
        for (int attempt = 0; attempt < 12 && guess > 0 && guess <= maxTargetSize; attempt++)
        {
            if (TryDecodeLz4(payload, guess, out decoded))
            {
                return true;
            }

            if (guess > maxTargetSize / 2)
            {
                break;
            }

            guess *= 2;
        }

        return false;
    }

    private static ChunkMaterialTotals ParseVoxelCellData(byte[] data)
    {
        var reader = new Reader(data);

        uint magic = reader.ReadU32();
        if (magic != MagicVoxelCellData)
        {
            throw new VoxelDeserializeException($"Bad VoxelCellData magic: 0x{magic:X8}");
        }

        _ = reader.ReadU32(); // version

        uint gridMagic = reader.ReadU32();
        if (gridMagic != MagicVertexGrid)
        {
            throw new VoxelDeserializeException($"Bad VertexGrid magic: 0x{gridMagic:X8}");
        }

        _ = reader.ReadU32(); // grid version

        _ = ReadI32Vec3(reader); // range origin
        (int sizeX, int sizeY, int sizeZ) = ReadI32Vec3(reader);
        _ = ReadI32Vec3(reader); // inner range origin
        _ = ReadI32Vec3(reader); // inner range size

        long voxelCount = (long)sizeX * sizeY * sizeZ;
        if (voxelCount < 0L)
        {
            throw new VoxelDeserializeException(
                $"Invalid range size ({sizeX}, {sizeY}, {sizeZ}) produced negative voxel count.");
        }

        var localMaterialCounts = new Dictionary<int, long>();

        long covered = 0L;
        while (covered < voxelCount)
        {
            byte hasMaterial = reader.ReadU8();
            int? materialId = hasMaterial != 0 ? reader.ReadU8() : null;
            int runLength = reader.ReadU8() + 1;

            covered += runLength;
            if (covered > voxelCount)
            {
                throw new VoxelDeserializeException("Sparse material runs exceed expected voxel count.");
            }

            if (materialId.HasValue)
            {
                int key = materialId.Value;
                localMaterialCounts[key] = localMaterialCounts.TryGetValue(key, out long current)
                    ? current + runLength
                    : runLength;
            }
        }

        covered = 0L;
        while (covered < voxelCount)
        {
            byte flags = reader.ReadU8();
            int runLength = reader.ReadU8() + 1;

            covered += runLength;
            if (covered > voxelCount)
            {
                throw new VoxelDeserializeException("Sparse vertex runs exceed expected voxel count.");
            }

            if ((flags & 1) == 0)
            {
                continue;
            }

            int innerCovered = 0;
            while (innerCovered < runLength)
            {
                reader.ReadExact(3);
                int innerRunLength = reader.ReadU8() + 1;
                innerCovered += innerRunLength;
                if (innerCovered > runLength)
                {
                    throw new VoxelDeserializeException("Sparse vertex inner runs exceed parent run length.");
                }
            }
        }

        uint mappingCount = reader.ReadU32();
        if (mappingCount > 256U)
        {
            throw new VoxelDeserializeException("Material mapping count exceeds local ID space (0-255).");
        }

        var materialMapping = new Dictionary<int, MaterialDefinition>();
        for (int i = 0; i < mappingCount; i++)
        {
            ulong gameMaterialId = reader.ReadU64();
            byte[] rawName = reader.ReadExact(8);
            string shortName = DecodeShortMaterialName(rawName);
            int localId = reader.ReadU8();

            materialMapping[localId] = new MaterialDefinition(
                gameMaterialId,
                string.IsNullOrWhiteSpace(shortName) ? "Unknown" : shortName);
        }

        _ = reader.ReadU8(); // is_diff

        var countsByMaterial = new Dictionary<MaterialAggregateKey, long>();
        foreach ((int localId, long count) in localMaterialCounts)
        {
            MaterialAggregateKey key;
            if (materialMapping.TryGetValue(localId, out MaterialDefinition definition))
            {
                key = new MaterialAggregateKey(
                    definition.GameMaterialId.ToString(CultureInfo.InvariantCulture),
                    definition.ShortName);
            }
            else
            {
                key = new MaterialAggregateKey($"local:{localId.ToString(CultureInfo.InvariantCulture)}", "Unknown");
            }

            countsByMaterial[key] = countsByMaterial.TryGetValue(key, out long current)
                ? current + count
                : count;
        }

        return new ChunkMaterialTotals(countsByMaterial);
    }

    private static string DecodeShortMaterialName(byte[] rawName)
    {
        int length = Array.IndexOf(rawName, (byte)0x00);
        int count = length >= 0 ? length : rawName.Length;
        return Encoding.UTF8.GetString(rawName, 0, count).Trim();
    }

    private static (int X, int Y, int Z) ReadI32Vec3(Reader reader)
    {
        return (reader.ReadI32(), reader.ReadI32(), reader.ReadI32());
    }

    private readonly record struct MaterialDefinition(
        ulong GameMaterialId,
        string ShortName);

    private readonly record struct ChunkMaterialTotals(
        IReadOnlyDictionary<MaterialAggregateKey, long> CountsByMaterial);

    private readonly record struct MaterialAggregateKey(
        string MaterialId,
        string MaterialName);

    private sealed class Reader
    {
        private readonly byte[] _buffer;
        private int _offset;

        public Reader(byte[] buffer)
        {
            _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            _offset = 0;
        }

        public byte ReadU8()
        {
            EnsureAvailable(1);
            return _buffer[_offset++];
        }

        public uint ReadU32()
        {
            EnsureAvailable(4);
            uint value = BinaryPrimitives.ReadUInt32LittleEndian(_buffer.AsSpan(_offset, 4));
            _offset += 4;
            return value;
        }

        public ulong ReadU64()
        {
            EnsureAvailable(8);
            ulong value = BinaryPrimitives.ReadUInt64LittleEndian(_buffer.AsSpan(_offset, 8));
            _offset += 8;
            return value;
        }

        public int ReadI32()
        {
            EnsureAvailable(4);
            int value = BinaryPrimitives.ReadInt32LittleEndian(_buffer.AsSpan(_offset, 4));
            _offset += 4;
            return value;
        }

        public byte[] ReadExact(int length)
        {
            EnsureAvailable(length);
            byte[] value = _buffer.AsSpan(_offset, length).ToArray();
            _offset += length;
            return value;
        }

        private void EnsureAvailable(int length)
        {
            if (length < 0 || _offset + length > _buffer.Length)
            {
                throw new VoxelDeserializeException(
                    $"Unexpected EOF while reading {length.ToString(CultureInfo.InvariantCulture)} byte(s).");
            }
        }
    }

    private sealed class VoxelDeserializeException : InvalidOperationException
    {
        public VoxelDeserializeException(string message)
            : base(message)
        {
        }

        public VoxelDeserializeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
