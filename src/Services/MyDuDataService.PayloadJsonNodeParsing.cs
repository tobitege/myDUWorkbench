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
    private static bool TryNormalizeVec3Node(JsonNode? node, out JsonObject result, out bool changed)
    {
        changed = false;
        result = new JsonObject();
        if (node is JsonObject obj &&
            TryGetDoubleByName(obj, "x", out double x) &&
            TryGetDoubleByName(obj, "y", out double y) &&
            TryGetDoubleByName(obj, "z", out double z))
        {
            result["x"] = JsonValue.Create(x);
            result["y"] = JsonValue.Create(y);
            result["z"] = JsonValue.Create(z);
            changed = !IsCanonicalVec3Object(obj, x, y, z);
            return true;
        }

        if (node is JsonArray arr &&
            arr.Count >= 3 &&
            TryGetDoubleFromJsonNode(arr[0], out double ax) &&
            TryGetDoubleFromJsonNode(arr[1], out double ay) &&
            TryGetDoubleFromJsonNode(arr[2], out double az))
        {
            result["x"] = JsonValue.Create(ax);
            result["y"] = JsonValue.Create(ay);
            result["z"] = JsonValue.Create(az);
            changed = true;
            return true;
        }

        return false;
    }

    private static bool IsCanonicalQuatObject(JsonObject obj, double w, double x, double y, double z)
    {
        if (obj.Count != 4)
        {
            return false;
        }

        return string.Equals(obj.ElementAt(0).Key, "w", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(1).Key, "x", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(2).Key, "y", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(3).Key, "z", StringComparison.Ordinal) &&
               TryGetDoubleFromJsonNode(obj.ElementAt(0).Value, out double cw) && Math.Abs(cw - w) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(1).Value, out double cx) && Math.Abs(cx - x) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(2).Value, out double cy) && Math.Abs(cy - y) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(3).Value, out double cz) && Math.Abs(cz - z) < 1e-9;
    }

    private static bool IsCanonicalVec3Object(JsonObject obj, double x, double y, double z)
    {
        if (obj.Count != 3)
        {
            return false;
        }

        return string.Equals(obj.ElementAt(0).Key, "x", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(1).Key, "y", StringComparison.Ordinal) &&
               string.Equals(obj.ElementAt(2).Key, "z", StringComparison.Ordinal) &&
               TryGetDoubleFromJsonNode(obj.ElementAt(0).Value, out double cx) && Math.Abs(cx - x) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(1).Value, out double cy) && Math.Abs(cy - y) < 1e-9 &&
               TryGetDoubleFromJsonNode(obj.ElementAt(2).Value, out double cz) && Math.Abs(cz - z) < 1e-9;
    }

    private static bool TryGetDoubleByName(JsonObject obj, string name, out double value)
    {
        if (TryGetJsonPropertyIgnoreCase(obj, name, out _, out JsonNode? node) &&
            TryGetDoubleFromJsonNode(node, out value))
        {
            return true;
        }

        value = 0d;
        return false;
    }

    private static bool TryGetIntFromJsonNode(JsonNode? node, out int value)
    {
        value = 0;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<int>(out int i))
        {
            value = i;
            return true;
        }

        if (scalar.TryGetValue<long>(out long l) && l >= int.MinValue && l <= int.MaxValue)
        {
            value = (int)l;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s) &&
            int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetLongFromJsonNode(JsonNode? node, out long value)
    {
        value = 0L;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<long>(out long l))
        {
            value = l;
            return true;
        }

        if (scalar.TryGetValue<int>(out int i))
        {
            value = i;
            return true;
        }

        if (scalar.TryGetValue<double>(out double d) && Math.Abs(d % 1d) < 1e-9 &&
            d >= long.MinValue && d <= long.MaxValue)
        {
            value = (long)d;
            return true;
        }

        if (scalar.TryGetValue<bool>(out bool b))
        {
            value = b ? 1L : 0L;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s) &&
            long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetDoubleFromJsonNode(JsonNode? node, out double value)
    {
        value = 0d;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<double>(out double d))
        {
            value = d;
            return true;
        }

        if (scalar.TryGetValue<long>(out long l))
        {
            value = l;
            return true;
        }

        if (scalar.TryGetValue<int>(out int i))
        {
            value = i;
            return true;
        }

        if (scalar.TryGetValue<bool>(out bool b))
        {
            value = b ? 1d : 0d;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s) &&
            double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryGetBoolFromJsonNode(JsonNode? node, out bool value)
    {
        value = false;
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<bool>(out bool b))
        {
            value = b;
            return true;
        }

        if (scalar.TryGetValue<long>(out long l))
        {
            value = l != 0L;
            return true;
        }

        if (scalar.TryGetValue<double>(out double d))
        {
            value = Math.Abs(d) > double.Epsilon;
            return true;
        }

        if (scalar.TryGetValue<string>(out string? s))
        {
            if (bool.TryParse(s, out bool parsedBool))
            {
                value = parsedBool;
                return true;
            }

            if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedLong))
            {
                value = parsedLong != 0L;
                return true;
            }
        }

        return false;
    }

    private static bool IsNodeBoolean(JsonNode? node, bool value)
    {
        return node is JsonValue scalar && scalar.TryGetValue<bool>(out bool existing) && existing == value;
    }

    private static bool IsNodeInteger(JsonNode? node, long value)
    {
        return node is JsonValue scalar && scalar.TryGetValue<long>(out long existing) && existing == value;
    }

    private static bool IsNodeDouble(JsonNode? node, double value)
    {
        if (node is not JsonValue scalar)
        {
            return false;
        }

        if (scalar.TryGetValue<double>(out double existing))
        {
            return Math.Abs(existing - value) < 1e-12;
        }

        if (scalar.TryGetValue<long>(out long existingLong))
        {
            return Math.Abs(existingLong - value) < 1e-12;
        }

        return false;
    }

    private static string ConvertJsonNodeToString(JsonNode? node)
    {
        if (node is null)
        {
            return string.Empty;
        }

        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<string>(out string? s))
            {
                return s ?? string.Empty;
            }

            if (scalar.TryGetValue<bool>(out bool b))
            {
                return b ? "true" : "false";
            }

            if (scalar.TryGetValue<long>(out long l))
            {
                return l.ToString(CultureInfo.InvariantCulture);
            }

            if (scalar.TryGetValue<double>(out double d))
            {
                return d.ToString("R", CultureInfo.InvariantCulture);
            }
        }

        return node.ToJsonString();
    }

    private static bool ApplyCreatorOverrideToBlueprintPayload(
        JsonNode root,
        ulong creatorPlayerId,
        ulong creatorOrganizationId,
        out string note)
    {
        note = string.Empty;
        if (creatorPlayerId == 0UL && creatorOrganizationId == 0UL)
        {
            return false;
        }

        if (root is not JsonObject rootObject ||
            !TryGetJsonPropertyIgnoreCase(rootObject, "model", out string modelKey, out JsonNode? modelNode))
        {
            note =
                "creator override requested but no blueprint model was found; endpoint query uses configured creator ids.";
            return true;
        }

        JsonObject modelObject = modelNode as JsonObject ?? new JsonObject();
        if (modelNode is not JsonObject)
        {
            rootObject[modelKey] = modelObject;
        }

        SetJsonPropertyCaseAware(
            modelObject,
            "CreatorId",
            JsonValue.Create(creatorPlayerId == 0UL ? 2UL : creatorPlayerId));

        JsonObject jsonPropertiesObject = GetOrCreateJsonObjectProperty(modelObject, "JsonProperties");
        JsonObject serverPropertiesObject = GetOrCreateJsonObjectProperty(jsonPropertiesObject, "serverProperties");

        JsonObject creatorIdObject = GetOrCreateJsonObjectProperty(serverPropertiesObject, "creatorId");
        SetJsonPropertyCaseAware(creatorIdObject, "playerId", JsonValue.Create(creatorPlayerId));
        SetJsonPropertyCaseAware(creatorIdObject, "organizationId", JsonValue.Create(creatorOrganizationId));

        note =
            $"applied creator override in payload (playerId={creatorPlayerId.ToString(CultureInfo.InvariantCulture)}, " +
            $"organizationId={creatorOrganizationId.ToString(CultureInfo.InvariantCulture)})";
        return true;
    }

    private static JsonObject GetOrCreateJsonObjectProperty(JsonObject owner, string propertyName)
    {
        if (TryGetJsonPropertyIgnoreCase(owner, propertyName, out string actualName, out JsonNode? value))
        {
            if (value is JsonObject existingObject)
            {
                return existingObject;
            }

            var replacement = new JsonObject();
            owner[actualName] = replacement;
            return replacement;
        }

        var created = new JsonObject();
        owner[propertyName] = created;
        return created;
    }

    private static void SetJsonPropertyCaseAware(JsonObject owner, string propertyName, JsonNode? value)
    {
        if (TryGetJsonPropertyIgnoreCase(owner, propertyName, out string actualName, out _))
        {
            owner[actualName] = value;
            return;
        }

        owner[propertyName] = value;
    }

    private static bool TryGetJsonPropertyIgnoreCase(
        JsonObject obj,
        string propertyName,
        out string actualName,
        out JsonNode? value)
    {
        foreach (KeyValuePair<string, JsonNode?> kvp in obj)
        {
            if (string.Equals(kvp.Key, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                actualName = kvp.Key;
                value = kvp.Value;
                return true;
            }
        }

        actualName = string.Empty;
        value = null;
        return false;
    }

    private static bool TryReadJsonString(JsonNode? node, out string value)
    {
        value = string.Empty;
        if (node is JsonValue scalar && scalar.TryGetValue<string>(out string? stringValue) &&
            !string.IsNullOrWhiteSpace(stringValue))
        {
            value = stringValue;
            return true;
        }

        return false;
    }
}
