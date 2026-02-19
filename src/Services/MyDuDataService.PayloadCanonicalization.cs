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
    private static JsonArray ConvertPropertyObjectToArray(JsonObject source)
    {
        var array = new JsonArray();
        foreach (KeyValuePair<string, JsonNode?> kvp in source)
        {
            JsonObject payload = CanonicalizePropertyPayload(kvp.Value, out _);
            array.Add(new JsonArray
            {
                JsonValue.Create(kvp.Key),
                payload
            });
        }

        return array;
    }

    private static int CanonicalizePropertyEntryArray(JsonArray propertyArray)
    {
        int fixes = 0;
        for (int index = 0; index < propertyArray.Count; index++)
        {
            JsonNode? entry = propertyArray[index];
            if (!TryCanonicalizePropertyArrayEntry(entry, index, out JsonArray canonicalEntry, out bool changed))
            {
                continue;
            }

            if (changed)
            {
                propertyArray[index] = canonicalEntry;
                fixes++;
            }
        }

        return fixes;
    }

    private static bool TryCanonicalizePropertyArrayEntry(
        JsonNode? entry,
        int index,
        out JsonArray canonicalEntry,
        out bool changed)
    {
        changed = false;
        string key = $"_idx{index.ToString(CultureInfo.InvariantCulture)}";
        JsonNode? rawPayload = null;

        if (entry is JsonArray pair)
        {
            if (pair.Count > 0 && TryReadJsonString(pair[0], out string parsedKey))
            {
                key = parsedKey;
            }
            else
            {
                changed = true;
            }

            if (pair.Count > 1)
            {
                rawPayload = pair[1];
            }
            else
            {
                changed = true;
            }

            if (pair.Count != 2)
            {
                changed = true;
            }
        }
        else if (entry is JsonObject obj)
        {
            if (TryGetJsonPropertyIgnoreCase(obj, "name", out _, out JsonNode? nameNode) &&
                TryReadJsonString(nameNode, out string parsedName))
            {
                key = parsedName;
            }
            else if (TryGetJsonPropertyIgnoreCase(obj, "key", out _, out JsonNode? keyNode) &&
                     TryReadJsonString(keyNode, out string parsedKey))
            {
                key = parsedKey;
            }
            else if (obj.Count == 1)
            {
                KeyValuePair<string, JsonNode?> single = obj.First();
                key = single.Key;
                rawPayload = single.Value;
                changed = true;
            }
            else
            {
                changed = true;
            }

            if (rawPayload is null)
            {
                if (TryGetJsonPropertyIgnoreCase(obj, "value", out _, out JsonNode? valueNode))
                {
                    rawPayload = valueNode;
                }
                else
                {
                    rawPayload = obj.DeepClone();
                    changed = true;
                }
            }
        }
        else
        {
            rawPayload = entry?.DeepClone();
            changed = true;
        }

        JsonObject canonicalPayload = CanonicalizePropertyPayload(rawPayload, out bool payloadChanged);
        changed |= payloadChanged;

        canonicalEntry = new JsonArray
        {
            JsonValue.Create(key),
            canonicalPayload
        };

        if (!changed &&
            entry is JsonArray originalPair &&
            originalPair.Count == 2 &&
            JsonNode.DeepEquals(originalPair[0], canonicalEntry[0]) &&
            JsonNode.DeepEquals(originalPair[1], canonicalEntry[1]))
        {
            return true;
        }

        if (!changed && entry is not JsonArray)
        {
            changed = true;
        }

        return true;
    }

    private static JsonObject CanonicalizePropertyPayload(JsonNode? payloadNode, out bool changed)
    {
        changed = false;
        int type;
        JsonNode? valueNode;

        if (payloadNode is JsonObject payloadObj &&
            TryGetJsonPropertyIgnoreCase(payloadObj, "type", out _, out JsonNode? typeNode))
        {
            if (!TryGetIntFromJsonNode(typeNode, out type))
            {
                type = InferPropertyTypeFromNode(TryGetValueNode(payloadObj));
                changed = true;
            }

            valueNode = TryGetValueNode(payloadObj);
            if (valueNode is null)
            {
                valueNode = null;
                changed = true;
            }

            if (!IsCanonicalPayloadObject(payloadObj))
            {
                changed = true;
            }
        }
        else
        {
            valueNode = payloadNode;
            type = InferPropertyTypeFromNode(valueNode);
            changed = true;
        }

        JsonNode? canonicalValue = CoercePropertyValueForType(ref type, valueNode, out bool valueChanged);
        changed |= valueChanged;

        var canonicalPayload = new JsonObject
        {
            ["type"] = JsonValue.Create(type),
            ["value"] = canonicalValue
        };

        if (!changed &&
            payloadNode is JsonObject originalObject &&
            !JsonNode.DeepEquals(originalObject, canonicalPayload))
        {
            changed = true;
        }

        return canonicalPayload;
    }

    private static JsonNode? TryGetValueNode(JsonObject payloadObj)
    {
        return TryGetJsonPropertyIgnoreCase(payloadObj, "value", out _, out JsonNode? valueNode)
            ? valueNode
            : null;
    }

    private static bool IsCanonicalPayloadObject(JsonObject payloadObj)
    {
        if (payloadObj.Count != 2)
        {
            return false;
        }

        KeyValuePair<string, JsonNode?> first = payloadObj.ElementAt(0);
        KeyValuePair<string, JsonNode?> second = payloadObj.ElementAt(1);
        return string.Equals(first.Key, "type", StringComparison.Ordinal) &&
               string.Equals(second.Key, "value", StringComparison.Ordinal);
    }

    private static int InferPropertyTypeFromNode(JsonNode? node)
    {
        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<bool>(out _))
            {
                return 1;
            }

            if (scalar.TryGetValue<long>(out _) || scalar.TryGetValue<int>(out _))
            {
                return 2;
            }

            if (scalar.TryGetValue<double>(out double floatValue))
            {
                return Math.Abs(floatValue % 1d) < 1e-9 ? 2 : 3;
            }

            return 4;
        }

        if (node is JsonArray array)
        {
            if (array.Count == 4)
            {
                return 5;
            }

            if (array.Count == 3)
            {
                return 6;
            }

            return 4;
        }

        if (node is JsonObject obj)
        {
            bool hasQuat = TryGetJsonPropertyIgnoreCase(obj, "w", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "x", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "y", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "z", out _, out _);
            if (hasQuat)
            {
                return 5;
            }

            bool hasVec3 = TryGetJsonPropertyIgnoreCase(obj, "x", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "y", out _, out _) &&
                           TryGetJsonPropertyIgnoreCase(obj, "z", out _, out _);
            if (hasVec3)
            {
                return 6;
            }
        }

        return 4;
    }

    private static JsonNode? CoercePropertyValueForType(ref int type, JsonNode? valueNode, out bool changed)
    {
        changed = false;
        switch (type)
        {
            case 1:
                if (TryGetBoolFromJsonNode(valueNode, out bool boolValue))
                {
                    if (!IsNodeBoolean(valueNode, boolValue))
                    {
                        changed = true;
                    }

                    return JsonValue.Create(boolValue);
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 2:
                if (TryGetLongFromJsonNode(valueNode, out long intValue))
                {
                    if (!IsNodeInteger(valueNode, intValue))
                    {
                        changed = true;
                    }

                    return JsonValue.Create(intValue);
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 3:
                if (TryGetDoubleFromJsonNode(valueNode, out double doubleValue))
                {
                    if (!IsNodeDouble(valueNode, doubleValue))
                    {
                        changed = true;
                    }

                    return JsonValue.Create(doubleValue);
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 5:
                if (TryNormalizeQuatNode(valueNode, out JsonObject quatNode, out bool quatChanged))
                {
                    changed = quatChanged;
                    return quatNode;
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 6:
                if (TryNormalizeVec3Node(valueNode, out JsonObject vec3Node, out bool vec3Changed))
                {
                    changed = vec3Changed;
                    return vec3Node;
                }

                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            case 4:
            case 7:
            case 255:
                if (valueNode is JsonValue scalar && scalar.TryGetValue<string>(out string? stringValue))
                {
                    return JsonValue.Create(stringValue ?? string.Empty);
                }

                changed = true;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
            default:
                changed = true;
                type = 4;
                return JsonValue.Create(ConvertJsonNodeToString(valueNode));
        }
    }

    private static bool TryNormalizeQuatNode(JsonNode? node, out JsonObject result, out bool changed)
    {
        changed = false;
        result = new JsonObject();
        if (node is JsonObject obj &&
            TryGetDoubleByName(obj, "w", out double w) &&
            TryGetDoubleByName(obj, "x", out double x) &&
            TryGetDoubleByName(obj, "y", out double y) &&
            TryGetDoubleByName(obj, "z", out double z))
        {
            result["w"] = JsonValue.Create(w);
            result["x"] = JsonValue.Create(x);
            result["y"] = JsonValue.Create(y);
            result["z"] = JsonValue.Create(z);
            changed = !IsCanonicalQuatObject(obj, w, x, y, z);
            return true;
        }

        if (node is JsonArray arr &&
            arr.Count >= 4 &&
            TryGetDoubleFromJsonNode(arr[0], out double aw) &&
            TryGetDoubleFromJsonNode(arr[1], out double ax) &&
            TryGetDoubleFromJsonNode(arr[2], out double ay) &&
            TryGetDoubleFromJsonNode(arr[3], out double az))
        {
            result["w"] = JsonValue.Create(aw);
            result["x"] = JsonValue.Create(ax);
            result["y"] = JsonValue.Create(ay);
            result["z"] = JsonValue.Create(az);
            changed = true;
            return true;
        }

        return false;
    }

    private static long EstimateJsonBase64RequestBodyLength(long payloadBytes)
    {
        if (payloadBytes <= 0)
        {
            return 2;
        }

        long base64Length = ((payloadBytes + 2L) / 3L) * 4L;
        return base64Length + 2L; // JSON string quotes.
    }
}
