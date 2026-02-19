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
    private static string GetPayloadKindDisplayName(ImportRequestPayloadKind payloadKind)
    {
        return payloadKind switch
        {
            ImportRequestPayloadKind.JsonBase64ByteArray => "json-base64-byte-array",
            _ => payloadKind.ToString()
        };
    }

    private enum ImportRequestPayloadKind
    {
        JsonBase64ByteArray
    }

    private static Uri BuildBlueprintImportEndpoint(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId)
    {
        Uri baseUri;
        if (!string.IsNullOrWhiteSpace(blueprintImportEndpoint))
        {
            string explicitCandidate = blueprintImportEndpoint.Trim()
                .Replace("{id}", "0", StringComparison.OrdinalIgnoreCase);
            if (!Uri.TryCreate(explicitCandidate, UriKind.Absolute, out Uri? parsedExplicitUri) || parsedExplicitUri is null)
            {
                throw new InvalidOperationException(
                    $"Blueprint import endpoint is not a valid absolute URI: {blueprintImportEndpoint}");
            }
            baseUri = parsedExplicitUri;

            if (string.IsNullOrWhiteSpace(baseUri.AbsolutePath) || baseUri.AbsolutePath == "/")
            {
                var explicitBuilder = new UriBuilder(baseUri)
                {
                    Path = "/blueprint/import"
                };
                baseUri = explicitBuilder.Uri;
            }
        }
        else
        {
            if (string.IsNullOrWhiteSpace(endpointTemplate))
            {
                throw new InvalidOperationException(
                    "Endpoint template is empty; cannot resolve blueprint import endpoint.");
            }

            string candidate = endpointTemplate.Trim()
                .Replace("{id}", "0", StringComparison.OrdinalIgnoreCase);
            if (!Uri.TryCreate(candidate, UriKind.Absolute, out Uri? parsedFallbackUri) || parsedFallbackUri is null)
            {
                throw new InvalidOperationException($"Endpoint template is not a valid absolute URI: {endpointTemplate}");
            }
            baseUri = parsedFallbackUri;

            var fallbackBuilder = new UriBuilder(baseUri)
            {
                Path = "/blueprint/import"
            };
            baseUri = fallbackBuilder.Uri;
        }

        var builder = new UriBuilder(baseUri);
        string existingQuery = string.IsNullOrWhiteSpace(builder.Query)
            ? string.Empty
            : builder.Query.TrimStart('?').Trim();
        string appendQuery =
            $"creatorPlayerId={creatorPlayerId.ToString(CultureInfo.InvariantCulture)}&" +
            $"creatorOrganizationId={creatorOrganizationId.ToString(CultureInfo.InvariantCulture)}";
        builder.Query = string.IsNullOrWhiteSpace(existingQuery)
            ? appendQuery
            : $"{existingQuery}&{appendQuery}";

        return builder.Uri;
    }

    private static IReadOnlyList<Uri> BuildBlueprintImportEndpointCandidates(
        string endpointTemplate,
        string? blueprintImportEndpoint,
        ulong creatorPlayerId,
        ulong creatorOrganizationId)
    {
        Uri primary = BuildBlueprintImportEndpoint(
            endpointTemplate,
            blueprintImportEndpoint,
            creatorPlayerId,
            creatorOrganizationId);

        var candidates = new List<Uri> { primary };

        if (TryBuildGameplayServiceFallbackEndpoint(primary, out Uri? fallback) && fallback is not null)
        {
            candidates.Add(fallback);
        }

        // Add loopback host variants because some local installs bind only IPv4 or only IPv6.
        int snapshotCount = candidates.Count;
        for (int i = 0; i < snapshotCount; i++)
        {
            Uri candidate = candidates[i];
            if (TryBuildLoopbackHostVariant(candidate, out Uri? loopbackVariant) && loopbackVariant is not null)
            {
                candidates.Add(loopbackVariant);
            }
        }

        var deduplicated = new List<Uri>(candidates.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Uri candidate in candidates)
        {
            string key = candidate.AbsoluteUri;
            if (seen.Add(key))
            {
                deduplicated.Add(candidate);
            }
        }

        return deduplicated;
    }

    private static bool TryBuildLoopbackHostVariant(Uri source, out Uri? loopbackVariant)
    {
        loopbackVariant = null;
        string host = source.Host;
        if (!string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var builder = new UriBuilder(source)
        {
            Host = "127.0.0.1"
        };
        loopbackVariant = builder.Uri;
        return true;
    }

    private static bool TryBuildGameplayServiceFallbackEndpoint(Uri primaryEndpoint, out Uri? fallbackEndpoint)
    {
        fallbackEndpoint = null;
        if (primaryEndpoint is null)
        {
            return false;
        }

        if (primaryEndpoint.Port != 12003)
        {
            return false;
        }

        var builder = new UriBuilder(primaryEndpoint)
        {
            Port = 10111,
            Path = "/blueprint/import"
        };
        fallbackEndpoint = builder.Uri;
        return true;
    }

    private static ulong? TryParseBlueprintIdFromImportResponse(string? responseText)
    {
        return TryParseBlueprintIdFromImportResponse(responseText, null, null);
    }

    private static ulong? TryParseBlueprintIdFromImportResponse(
        string? responseText,
        byte[]? responseBytes,
        string? responseMediaType)
    {
        if (!string.IsNullOrWhiteSpace(responseText))
        {
            string trimmed = responseText.Trim();
            if (ulong.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong direct))
            {
                return NormalizeBlueprintId(direct);
            }

            try
            {
                using JsonDocument document = JsonDocument.Parse(trimmed);
                JsonElement root = document.RootElement;

                if (root.ValueKind == JsonValueKind.Number && root.TryGetUInt64(out ulong numericRoot))
                {
                    return NormalizeBlueprintId(numericRoot);
                }

                if (root.ValueKind == JsonValueKind.Object)
                {
                    if (TryReadUInt64(root, "blueprintId", "BlueprintId", "id", "Id") is ulong id)
                    {
                        return NormalizeBlueprintId(id);
                    }
                }
            }
            catch
            {
            }
        }

        if (responseBytes is null || responseBytes.Length == 0)
        {
            return null;
        }

        if (!IsNovaquarkBinaryMediaType(responseMediaType))
        {
            return null;
        }

        if (!TryDecodeVarUInt64(responseBytes, out ulong rawValue))
        {
            return null;
        }

        // `application/vnd.novaquark.binary` responses carry a zig-zag encoded integer id.
        return NormalizeBlueprintId(rawValue >> 1);
    }

    private static string BuildImportResponsePreview(byte[] responseBytes, string? responseMediaType)
    {
        if (responseBytes is null || responseBytes.Length == 0)
        {
            return string.Empty;
        }

        if (IsNovaquarkBinaryMediaType(responseMediaType))
        {
            string hex = Convert.ToHexString(responseBytes);
            if (TryDecodeVarUInt64(responseBytes, out ulong raw))
            {
                ulong decodedId = raw >> 1;
                return $"binary(varint={raw.ToString(CultureInfo.InvariantCulture)}, decodedId={decodedId.ToString(CultureInfo.InvariantCulture)}, hex={hex})";
            }

            return $"binary(hex={hex})";
        }

        return DecodeResponseText(responseBytes);
    }

    private static string DecodeResponseText(byte[] responseBytes)
    {
        if (responseBytes is null || responseBytes.Length == 0)
        {
            return string.Empty;
        }

        string utf8 = Encoding.UTF8.GetString(responseBytes);
        if (utf8.IndexOf('\0') >= 0)
        {
            return Convert.ToBase64String(responseBytes);
        }

        return utf8;
    }

    private static bool IsNovaquarkBinaryMediaType(string? mediaType)
    {
        return !string.IsNullOrWhiteSpace(mediaType) &&
               mediaType.Contains("application/vnd.novaquark.binary", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryDecodeVarUInt64(byte[] bytes, out ulong value)
    {
        value = 0UL;
        if (bytes is null || bytes.Length == 0)
        {
            return false;
        }

        int shift = 0;
        for (int i = 0; i < bytes.Length && i < 10; i++)
        {
            byte current = bytes[i];
            value |= (ulong)(current & 0x7F) << shift;
            if ((current & 0x80) == 0)
            {
                return true;
            }

            shift += 7;
        }

        value = 0UL;
        return false;
    }

    private static string BuildHttpBodyPreview(string? responseText)
    {
        if (string.IsNullOrWhiteSpace(responseText))
        {
            return "empty response body";
        }

        string[] tokens = responseText
            .Split(new[] {'\r', '\n', '\t'}, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        string singleLine = tokens.Length == 0 ? responseText.Trim() : string.Join(" ", tokens);
        const int maxLength = 320;
        if (singleLine.Length <= maxLength)
        {
            return singleLine;
        }

        return singleLine[..(maxLength - 3)] + "...";
    }

    private static BlueprintImportResult ParseBlueprintJsonLegacy(string jsonContent, string sourceName, string? serverRootPath)
    {
        using JsonDocument document = JsonDocument.Parse(jsonContent, CreateBlueprintJsonDocumentOptions());
        return ParseBlueprintJsonLegacy(document.RootElement, sourceName, serverRootPath);
    }

    private static BlueprintImportResult ParseBlueprintJsonLegacy(Stream jsonStream, string sourceName, string? serverRootPath)
    {
        if (jsonStream is null)
        {
            throw new ArgumentNullException(nameof(jsonStream));
        }

        if (jsonStream.CanSeek && jsonStream.Position != 0L)
        {
            jsonStream.Seek(0L, SeekOrigin.Begin);
        }

        using JsonDocument document = JsonDocument.Parse(jsonStream, CreateBlueprintJsonDocumentOptions());
        return ParseBlueprintJsonLegacy(document.RootElement, sourceName, serverRootPath);
    }

    private static BlueprintImportResult ParseBlueprintJsonLegacy(JsonElement root, string sourceName, string? serverRootPath)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException("Blueprint root must be a JSON object.");
        }

        JsonElement model = default;
        bool hasModel = TryGetPropertyIgnoreCase(root, "Model", out model) && model.ValueKind == JsonValueKind.Object;
        JsonElement elements = default;
        bool hasElements = TryGetPropertyIgnoreCase(root, "Elements", out elements) && elements.ValueKind == JsonValueKind.Array;

        ulong? blueprintId = hasModel
            ? NormalizeBlueprintId(TryReadUInt64(model, "Id", "id", "blueprintId", "blueprint_id"))
            : null;
        string blueprintName = hasModel ? TryReadString(model, "Name", "name") ?? string.Empty : string.Empty;
        if (string.IsNullOrWhiteSpace(blueprintName))
        {
            blueprintName = string.IsNullOrWhiteSpace(sourceName) ? "Blueprint import" : sourceName;
        }

        var records = new List<ElementPropertyRecord>();
        int elementCount = 0;

        if (hasElements)
        {
            int fallbackElementId = 0;
            foreach (JsonElement element in elements.EnumerateArray())
            {
                if (element.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                fallbackElementId++;
                ulong elementId = TryReadUInt64(element, "elementId", "element_id", "id") ?? (ulong)fallbackElementId;
                string elementDisplayName = BuildBlueprintElementDisplayName(element, elementId);

                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (string.Equals(property.Name, "properties", StringComparison.OrdinalIgnoreCase) &&
                        property.Value.ValueKind == JsonValueKind.Array &&
                        TryExpandBlueprintElementProperties(records, elementId, elementDisplayName, property.Value, serverRootPath))
                    {
                        continue;
                    }

                    AddBlueprintPropertyRecord(
                        records,
                        elementId,
                        elementDisplayName,
                        property.Name,
                        property.Value,
                        propertyTypeOverride: null,
                        serverRootPath: serverRootPath);
                }

                elementCount++;
            }
        }

        const ulong modelPseudoElementId = 0UL;
        if (hasModel)
        {
            foreach (JsonProperty property in model.EnumerateObject())
            {
                AddBlueprintPropertyRecord(
                    records,
                    modelPseudoElementId,
                    "BlueprintModel [0]",
                    $"model.{property.Name}",
                    property.Value,
                    propertyTypeOverride: null,
                    serverRootPath: serverRootPath);
            }
        }

        foreach (JsonProperty property in root.EnumerateObject())
        {
            if (string.Equals(property.Name, "Elements", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(property.Name, "Model", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            AddBlueprintPropertyRecord(
                records,
                modelPseudoElementId,
                "BlueprintRoot [0]",
                $"root.{property.Name}",
                property.Value,
                propertyTypeOverride: null,
                serverRootPath: serverRootPath);
        }

        if (records.Count == 0)
        {
            throw new InvalidOperationException("No readable element or model properties were found in blueprint JSON.");
        }

        return new BlueprintImportResult(
            sourceName ?? string.Empty,
            blueprintName,
            blueprintId,
            elementCount,
            records,
            string.Empty,
            string.Empty);
    }

    private static JsonDocumentOptions CreateBlueprintJsonDocumentOptions()
    {
        return new JsonDocumentOptions
        {
            AllowTrailingCommas = true,
            CommentHandling = JsonCommentHandling.Skip
        };
    }

    private static string FormatByteLength(long length)
    {
        const double kib = 1024d;
        const double mib = kib * 1024d;
        const double gib = mib * 1024d;

        if (length < kib)
        {
            return $"{length.ToString(CultureInfo.InvariantCulture)} B";
        }

        if (length < mib)
        {
            return $"{(length / kib).ToString("0.##", CultureInfo.InvariantCulture)} KiB";
        }

        if (length < gib)
        {
            return $"{(length / mib).ToString("0.##", CultureInfo.InvariantCulture)} MiB";
        }

        return $"{(length / gib).ToString("0.##", CultureInfo.InvariantCulture)} GiB";
    }

    private sealed record NqBlueprintProbe(
        bool Success,
        bool DllUnavailable,
        string Message,
        string DllPath,
        ulong? BlueprintId,
        string BlueprintName,
        int ElementCount,
        int LinkCount,
        bool HasVoxelData);

    private static NqBlueprintProbe ProbeBlueprintWithNqDll(
        string jsonContent,
        string? serverRootPath,
        string? nqUtilsDllPath)
    {
        if (!TryResolveNqUtilsDllPath(serverRootPath, nqUtilsDllPath, out string dllPath, out string resolveMessage))
        {
            return new NqBlueprintProbe(
                Success: false,
                DllUnavailable: true,
                Message: resolveMessage,
                DllPath: string.Empty,
                BlueprintId: null,
                BlueprintName: string.Empty,
                ElementCount: 0,
                LinkCount: 0,
                HasVoxelData: false);
        }

        try
        {
            Assembly nqAssembly = LoadNqUtilsAssembly(dllPath);
            Type? blueprintType = nqAssembly.GetType("NQ.BlueprintData", throwOnError: false);
            if (blueprintType is null)
            {
                return new NqBlueprintProbe(
                    Success: false,
                    DllUnavailable: false,
                    Message: "Type NQ.BlueprintData was not found in NQutils.dll.",
                    DllPath: dllPath,
                    BlueprintId: null,
                    BlueprintName: string.Empty,
                    ElementCount: 0,
                    LinkCount: 0,
                    HasVoxelData: false);
            }

            object? blueprint = JsonConvert.DeserializeObject(jsonContent, blueprintType);
            if (blueprint is null)
            {
                return new NqBlueprintProbe(
                    Success: false,
                    DllUnavailable: false,
                    Message: "JsonConvert returned null when deserializing NQ.BlueprintData.",
                    DllPath: dllPath,
                    BlueprintId: null,
                    BlueprintName: string.Empty,
                    ElementCount: 0,
                    LinkCount: 0,
                    HasVoxelData: false);
            }

            object? model = GetObjectProperty(blueprint, "Model");
            if (model is null)
            {
                return new NqBlueprintProbe(
                    Success: false,
                    DllUnavailable: false,
                    Message: "NQ.BlueprintData.Model is null after deserialization.",
                    DllPath: dllPath,
                    BlueprintId: null,
                    BlueprintName: string.Empty,
                    ElementCount: 0,
                    LinkCount: 0,
                    HasVoxelData: GetObjectProperty(blueprint, "VoxelData") is not null);
            }

            ulong? blueprintId = NormalizeBlueprintId(TryConvertToUInt64(GetObjectProperty(model, "Id")));
            string blueprintName = Convert.ToString(GetObjectProperty(model, "Name"), CultureInfo.InvariantCulture) ?? string.Empty;
            int elementCount = CountEnumerable(GetObjectProperty(blueprint, "Elements"));
            int linkCount = CountEnumerable(GetObjectProperty(blueprint, "Links"));
            bool hasVoxelData = GetObjectProperty(blueprint, "VoxelData") is not null;

            return new NqBlueprintProbe(
                Success: true,
                DllUnavailable: false,
                Message: "Validated with NQutils.dll.",
                DllPath: dllPath,
                BlueprintId: blueprintId,
                BlueprintName: blueprintName,
                ElementCount: elementCount,
                LinkCount: linkCount,
                HasVoxelData: hasVoxelData);
        }
        catch (Exception ex)
        {
            return new NqBlueprintProbe(
                Success: false,
                DllUnavailable: false,
                Message: BuildNqPreflightWarningMessage(ex),
                DllPath: dllPath,
                BlueprintId: null,
                BlueprintName: string.Empty,
                ElementCount: 0,
                LinkCount: 0,
                HasVoxelData: false);
        }
    }

    private static string BuildNqPreflightWarningMessage(Exception ex)
    {
        if (ex is JsonSerializationException jsonEx)
        {
            string path = string.IsNullOrWhiteSpace(jsonEx.Path) ? "<unknown>" : jsonEx.Path;
            string line = jsonEx.LineNumber > 0 ? jsonEx.LineNumber.ToString(CultureInfo.InvariantCulture) : "?";
            string position = jsonEx.LinePosition > 0 ? jsonEx.LinePosition.ToString(CultureInfo.InvariantCulture) : "?";

            if (path.Contains("serverProperties", StringComparison.OrdinalIgnoreCase))
            {
                return
                    $"Schema mismatch at '{path}' (line {line}, position {position}): " +
                    "serverProperties is an array, but NQ preflight expects an object dictionary.";
            }

            return $"Schema mismatch at '{path}' (line {line}, position {position}): {ExtractFirstSentence(jsonEx.Message)}";
        }

        return ExtractFirstSentence(ex.Message);
    }

    private static string ExtractFirstSentence(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return "unknown preflight error";
        }

        string flattened = message.Replace("\r", " ").Replace("\n", " ").Trim();
        int periodIndex = flattened.IndexOf('.', StringComparison.Ordinal);
        if (periodIndex > 0 && periodIndex < flattened.Length - 1)
        {
            return flattened[..(periodIndex + 1)].Trim();
        }

        const int maxLength = 280;
        if (flattened.Length <= maxLength)
        {
            return flattened;
        }

        return flattened[..(maxLength - 3)] + "...";
    }

    private static bool TryResolveNqUtilsDllPath(
        string? serverRootPath,
        string? nqUtilsDllPath,
        out string dllPath,
        out string message)
    {
        var candidates = new List<string>();
        if (!string.IsNullOrWhiteSpace(nqUtilsDllPath))
        {
            candidates.Add(nqUtilsDllPath);
        }

        string? pathFromEnv = Environment.GetEnvironmentVariable("MYDU_NQUTILS_DLL_PATH");
        if (!string.IsNullOrWhiteSpace(pathFromEnv))
        {
            candidates.Add(pathFromEnv);
        }

        string? dirFromEnv = Environment.GetEnvironmentVariable("MYDU_NQUTILS_DLL_DIR");
        if (!string.IsNullOrWhiteSpace(dirFromEnv))
        {
            candidates.Add(Path.Combine(dirFromEnv, "NQutils.dll"));
        }

        if (!string.IsNullOrWhiteSpace(serverRootPath))
        {
            candidates.Add(Path.Combine(serverRootPath, "wincs", "all", "NQutils.dll"));
        }

        candidates.AddRange(DefaultNqUtilsDllPaths);

        foreach (string candidate in candidates.Where(path => !string.IsNullOrWhiteSpace(path)))
        {
            string fullPath;
            try
            {
                fullPath = Path.GetFullPath(candidate);
            }
            catch
            {
                continue;
            }

            if (File.Exists(fullPath))
            {
                dllPath = fullPath;
                message = string.Empty;
                return true;
            }
        }

        dllPath = string.Empty;
        message =
            "NQutils.dll not found. Configure an explicit NQutils.dll path in the Config tab, " +
            "or set MYDU_NQUTILS_DLL_PATH / MYDU_NQUTILS_DLL_DIR, " +
            "or point Server Root Path to your myDU server folder.";
        return false;
    }

    private static Assembly LoadNqUtilsAssembly(string dllPath)
    {
        string fullPath = Path.GetFullPath(dllPath);
        Assembly? loaded = AppDomain.CurrentDomain.GetAssemblies()
            .FirstOrDefault(a =>
            {
                try
                {
                    string location = a.Location ?? string.Empty;
                    return location.Length > 0 &&
                           string.Equals(Path.GetFullPath(location), fullPath, StringComparison.OrdinalIgnoreCase);
                }
                catch
                {
                    return false;
                }
            });

        return loaded ?? Assembly.LoadFrom(fullPath);
    }

    private static object? GetObjectProperty(object target, string propertyName)
    {
        if (target is null)
        {
            return null;
        }

        PropertyInfo? property = target.GetType().GetProperty(
            propertyName,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

        return property?.GetValue(target);
    }

    private static int CountEnumerable(object? value)
    {
        if (value is null)
        {
            return 0;
        }

        if (value is ICollection collection)
        {
            return collection.Count;
        }

        if (value is IEnumerable enumerable)
        {
            int count = 0;
            foreach (object? _ in enumerable)
            {
                count++;
            }

            return count;
        }

        return 0;
    }

    private static ulong? TryConvertToUInt64(object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value is ulong u)
        {
            return u;
        }

        if (value is long l && l >= 0)
        {
            return (ulong)l;
        }

        if (value is int i && i >= 0)
        {
            return (ulong)i;
        }

        return ulong.TryParse(
            Convert.ToString(value, CultureInfo.InvariantCulture),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out ulong parsed)
            ? parsed
            : null;
    }

    private static ulong? NormalizeBlueprintId(ulong? blueprintId)
    {
        return blueprintId.HasValue && blueprintId.Value > 0UL
            ? blueprintId
            : null;
    }

}
