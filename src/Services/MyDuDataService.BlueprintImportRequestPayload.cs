using myDUWorker.Models;
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

namespace myDUWorker.Services;

public sealed partial class MyDuDataService
{
    private static string AppendRequestNotes(string currentNotes, string additionalNote)
    {
        if (string.IsNullOrWhiteSpace(additionalNote))
        {
            return currentNotes;
        }

        if (string.IsNullOrWhiteSpace(currentNotes))
        {
            return additionalNote.Trim();
        }

        string trimmedCurrent = currentNotes.Trim();
        return trimmedCurrent.EndsWith(".", StringComparison.Ordinal)
            ? $"{trimmedCurrent} {additionalNote.Trim()}"
            : $"{trimmedCurrent}; {additionalNote.Trim()}";
    }

    private static bool ShouldAttemptNoVoxelDataFallback(Exception ex)
    {
        if (ex is HttpRequestException httpEx && IsConnectionResetException(httpEx))
        {
            return true;
        }

        if (ex is InvalidOperationException invalidOperationException)
        {
            string message = invalidOperationException.Message;
            if (message.Contains("closed the connection during blueprint import", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("Unknown Exception got in server", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return ex.InnerException is not null && ShouldAttemptNoVoxelDataFallback(ex.InnerException);
    }

    private static bool TryBuildNoVoxelDataFallbackPayload(
        byte[] sourcePayload,
        out byte[] fallbackPayload,
        out string fallbackNote)
    {
        fallbackPayload = sourcePayload;
        fallbackNote = string.Empty;

        try
        {
            string jsonText = Encoding.UTF8.GetString(sourcePayload);
            JsonNode? root = JsonNode.Parse(jsonText);
            if (root is not JsonObject rootObject)
            {
                return false;
            }

            if (!TryGetJsonPropertyIgnoreCase(rootObject, "VoxelData", out string voxelDataName, out JsonNode? voxelDataNode))
            {
                return false;
            }

            int voxelEntryCount = voxelDataNode is JsonArray voxelArray ? voxelArray.Count : -1;
            if (voxelDataNode is JsonArray existingVoxelArray && existingVoxelArray.Count == 0)
            {
                return false;
            }

            rootObject[voxelDataName] = new JsonArray();
            string fallbackJson = rootObject.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false
            });

            byte[] candidatePayload = Encoding.UTF8.GetBytes(fallbackJson);
            if (candidatePayload.Length == 0 || sourcePayload.SequenceEqual(candidatePayload))
            {
                return false;
            }

            fallbackPayload = candidatePayload;
            string countText = voxelEntryCount >= 0
                ? $"{voxelEntryCount.ToString(CultureInfo.InvariantCulture)} entries"
                : "non-array value";
            fallbackNote =
                $"Fallback applied: stripped top-level VoxelData ({countText}) after backend voxel import failure.";
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildEndpointAttemptError(Exception ex)
    {
        string message = ex.Message;
        if (message.Length > 260)
        {
            return message[..257] + "...";
        }

        return message;
    }

    private static HttpRequestMessage BuildBlueprintImportRequest(
        Uri endpoint,
        byte[] blueprintJsonUtf8Payload,
        ImportRequestPayloadKind payloadKind)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
        };
        request.Headers.ConnectionClose = true;
        request.Headers.ExpectContinue = false;

        switch (payloadKind)
        {
            case ImportRequestPayloadKind.JsonBase64ByteArray:
            {
                string encodedPayload = Convert.ToBase64String(blueprintJsonUtf8Payload);
                string requestBody = System.Text.Json.JsonSerializer.Serialize(encodedPayload);
                request.Content = new StringContent(requestBody, Encoding.UTF8, "application/json");
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported import payload kind: {payloadKind}");
        }

        return request;
    }

    private static (byte[] Payload, string Notes) PrepareBlueprintPayloadForGameDatabaseImport(
        byte[] originalPayload,
        ulong creatorPlayerId,
        ulong creatorOrganizationId)
    {
        if (originalPayload.Length == 0)
        {
            return (originalPayload, string.Empty);
        }

        try
        {
            string jsonText = Encoding.UTF8.GetString(originalPayload);
            JsonNode? root = JsonNode.Parse(jsonText);
            if (root is null)
            {
                return (originalPayload, string.Empty);
            }

            var notes = new List<string>();
            int normalizedElementMaps = NormalizeElementPropertyMaps(
                root,
                out int removedMalformedServerProperties);
            if (normalizedElementMaps > 0)
            {
                notes.Add(
                    $"normalized {normalizedElementMaps.ToString(CultureInfo.InvariantCulture)} element property maps");
            }
            if (removedMalformedServerProperties > 0)
            {
                notes.Add(
                    $"repaired malformed serverProperties in {removedMalformedServerProperties.ToString(CultureInfo.InvariantCulture)} elements");
            }

            string normalizedJson = root.ToJsonString(new JsonSerializerOptions
            {
                WriteIndented = false
            });
            byte[] normalizedPayload = Encoding.UTF8.GetBytes(normalizedJson);
            if (normalizedPayload.Length < originalPayload.Length)
            {
                notes.Add(
                    $"minified JSON payload from {FormatByteLength(originalPayload.LongLength)} to {FormatByteLength(normalizedPayload.LongLength)}");
            }

            long estimatedRequestBodyBytes = EstimateJsonBase64RequestBodyLength(normalizedPayload.LongLength);
            if (estimatedRequestBodyBytes > EstimatedDefaultJsonRequestBodyLimitBytes)
            {
                notes.Add(
                    $"request body remains large after base64 (~{FormatByteLength(estimatedRequestBodyBytes)}); backend may reject it unless request size limits are increased");
            }

            bool payloadChanged = normalizedElementMaps > 0 ||
                                  removedMalformedServerProperties > 0 ||
                                  normalizedPayload.Length < originalPayload.Length;
            if (!payloadChanged)
            {
                return (originalPayload, string.Empty);
            }

            string noteText = notes.Count == 0
                ? "Runtime normalization applied."
                : $"Runtime normalization applied: {string.Join("; ", notes)}.";
            return (normalizedPayload, noteText);
        }
        catch (Exception ex)
        {
            return (
                originalPayload,
                $"Runtime normalization skipped: {BuildSingleLineExceptionPreview(ex)}");
        }
    }
}
