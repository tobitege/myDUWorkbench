// Helper Index:
// - TryDecode: Resolves hash-backed blobs, optionally LZ4-decompresses, and returns printable content text.
// - TryResolveHashBlob: Maps hash references to files under data/user_content and loads payload bytes.
// - TryDecodeLz4Payload: Validates and decodes block payloads with embedded uncompressed size.
// - IsMostlyPrintable: Guards against returning binary garbage as plain text.
using K4os.Compression.LZ4;
using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace MyDu.Services;

public sealed record ContentBlobDecodeResult(
    string DecodedText,
    int DbValueBytes,
    int PayloadBytes,
    int DecodedBytes,
    bool UsedLz4,
    string? SourceBlobPath);

public static class ContentBlobDecoder
{
    private static readonly Regex HashRegex = new("^[0-9a-f]{64}$", RegexOptions.Compiled);

    public static bool TryDecode(byte[] dbValue, string serverRootPath, out ContentBlobDecodeResult? result, out string? error)
    {
        result = null;
        error = null;

        if (dbValue.Length == 0)
        {
            error = "content payload is empty.";
            return false;
        }

        byte[] payload = dbValue;
        string? sourceBlobPath = null;
        if (TryResolveHashBlob(dbValue, serverRootPath, out byte[] resolvedPayload, out string? resolvedPath))
        {
            payload = resolvedPayload;
            sourceBlobPath = resolvedPath;
        }

        byte[] decoded = payload;
        bool usedLz4 = TryDecodeLz4Payload(payload, out byte[] lz4Decoded);
        if (usedLz4)
        {
            decoded = lz4Decoded;
        }

        string decodedText = Encoding.UTF8.GetString(decoded).Trim('\0');
        if (!IsMostlyPrintable(decodedText))
        {
            error = "decoded content is mostly non-printable.";
            return false;
        }

        result = new ContentBlobDecodeResult(
            decodedText,
            dbValue.Length,
            payload.Length,
            decoded.Length,
            usedLz4,
            sourceBlobPath);

        return true;
    }

    private static bool TryResolveHashBlob(byte[] value, string serverRootPath, out byte[] payload, out string? blobPath)
    {
        payload = value;
        blobPath = null;

        string text;
        try
        {
            text = Encoding.UTF8.GetString(value).Trim();
        }
        catch
        {
            return false;
        }

        if (!HashRegex.IsMatch(text))
        {
            return false;
        }

        string path = Path.Combine(serverRootPath, "data", "user_content", text);
        if (!File.Exists(path))
        {
            return false;
        }

        payload = File.ReadAllBytes(path);
        blobPath = path;
        return true;
    }

    private static bool TryDecodeLz4Payload(byte[] blob, out byte[] decoded)
    {
        decoded = Array.Empty<byte>();
        if (blob.Length < 8)
        {
            return false;
        }

        int uncompressedSize = BitConverter.ToInt32(blob, 0);
        if (uncompressedSize <= 0)
        {
            return false;
        }

        var output = new byte[uncompressedSize];
        int decodedLength = LZ4Codec.Decode(
            blob,
            4,
            blob.Length - 4,
            output,
            0,
            uncompressedSize);

        if (decodedLength != uncompressedSize)
        {
            return false;
        }

        decoded = output;
        return true;
    }

    private static bool IsMostlyPrintable(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return true;
        }

        int printable = 0;
        foreach (char c in value)
        {
            if (!char.IsControl(c) || c == '\r' || c == '\n' || c == '\t')
            {
                printable++;
            }
        }

        return printable >= (value.Length * 8 / 10);
    }
}
