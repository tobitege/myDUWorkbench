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
    private static int NormalizeElementPropertyMaps(
        JsonNode root,
        out int removedMalformedServerProperties)
    {
        removedMalformedServerProperties = 0;
        if (root is not JsonObject rootObject)
        {
            return 0;
        }

        if (!TryGetJsonPropertyIgnoreCase(rootObject, "elements", out string elementsKey, out JsonNode? elementsNode) ||
            elementsNode is not JsonArray elementsArray)
        {
            return 0;
        }

        int normalizedCount = 0;
        for (int i = 0; i < elementsArray.Count; i++)
        {
            if (elementsArray[i] is not JsonObject elementObject)
            {
                continue;
            }

            normalizedCount += NormalizeElementPropertiesField(elementObject);
            normalizedCount += NormalizeElementServerPropertiesField(
                elementObject,
                out bool removedServerProperties);
            if (removedServerProperties)
            {
                removedMalformedServerProperties++;
            }
        }

        // Keep case/style as found in source document.
        rootObject[elementsKey] = elementsArray;
        return normalizedCount;
    }

    private static int NormalizeElementPropertiesField(JsonObject elementObject)
    {
        if (!TryGetJsonPropertyIgnoreCase(elementObject, "properties", out string actualName, out JsonNode? node))
        {
            return 0;
        }

        // ElementInfo.properties uses PropertyMapConverter and must stay in array form:
        // [ ["name", { "type": ..., "value": ... }], ... ].
        if (node is JsonArray propertyArray)
        {
            int fixes = CanonicalizePropertyEntryArray(propertyArray);
            if (fixes > 0)
            {
                elementObject[actualName] = propertyArray;
            }

            return fixes;
        }

        if (node is JsonObject propertyMapObject)
        {
            JsonArray converted = ConvertPropertyObjectToArray(propertyMapObject);
            elementObject[actualName] = converted;
            return 1;
        }

        if (node is null)
        {
            elementObject[actualName] = new JsonArray();
            return 1;
        }

        // Unsupported scalar payload: reset to empty, valid property map array.
        elementObject[actualName] = new JsonArray();
        return 1;
    }

    private static int NormalizeElementServerPropertiesField(
        JsonObject elementObject,
        out bool removedServerProperties)
    {
        removedServerProperties = false;
        if (!TryGetJsonPropertyIgnoreCase(elementObject, "serverProperties", out string actualName, out JsonNode? node))
        {
            return 0;
        }

        if (node is JsonObject)
        {
            int fixes = NormalizePropertyMapObjectValues((JsonObject)node);
            if (fixes > 0)
            {
                elementObject[actualName] = node;
            }

            return fixes;
        }

        if (node is JsonArray arrayNode)
        {
            JsonObject converted = ConvertPropertyArrayToObject(arrayNode, out int droppedEntries);
            elementObject[actualName] = converted;
            return 1 + droppedEntries;
        }

        if (node is null)
        {
            elementObject[actualName] = new JsonObject();
            removedServerProperties = true;
            return 1;
        }

        // serverProperties is a Dictionary<string, PropertyValue> without PropertyMapConverter.
        // Scalars are irrecoverable for that shape; coerce to empty object map.
        elementObject[actualName] = new JsonObject();
        removedServerProperties = true;
        return 1;
    }

    private static int NormalizePropertyMapObjectValues(JsonObject propertyMap)
    {
        if (propertyMap.Count == 0)
        {
            return 0;
        }

        var toRemove = new List<string>();
        var toReplace = new List<KeyValuePair<string, JsonNode?>>();

        foreach (KeyValuePair<string, JsonNode?> kvp in propertyMap)
        {
            if (string.IsNullOrWhiteSpace(kvp.Key) || kvp.Value is null)
            {
                toRemove.Add(kvp.Key);
                continue;
            }

            JsonNode normalized = NormalizePropertyPayloadForMap(kvp.Value, out bool payloadChanged);
            if (payloadChanged || !JsonNode.DeepEquals(kvp.Value, normalized))
            {
                toReplace.Add(new KeyValuePair<string, JsonNode?>(kvp.Key, normalized));
            }
        }

        int fixes = toRemove.Count + toReplace.Count;
        if (fixes == 0)
        {
            return 0;
        }

        foreach (string key in toRemove)
        {
            propertyMap.Remove(key);
        }

        foreach (KeyValuePair<string, JsonNode?> kvp in toReplace)
        {
            propertyMap[kvp.Key] = kvp.Value;
        }

        return fixes;
    }

    private static JsonObject ConvertPropertyArrayToObject(JsonArray source, out int droppedEntries)
    {
        droppedEntries = 0;
        var map = new JsonObject();
        foreach (JsonNode? entry in source)
        {
            if (!TryReadPropertyMapEntry(entry, out string key, out JsonNode? payloadNode) ||
                string.IsNullOrWhiteSpace(key) ||
                payloadNode is null)
            {
                droppedEntries++;
                continue;
            }

            JsonNode normalized = NormalizePropertyPayloadForMap(payloadNode, out _);
            map[key] = normalized;
        }

        return map;
    }

    private static bool TryReadPropertyMapEntry(
        JsonNode? entry,
        out string key,
        out JsonNode? payloadNode)
    {
        key = string.Empty;
        payloadNode = null;

        if (entry is JsonArray pair)
        {
            if (pair.Count < 2 || !TryReadNonEmptyJsonString(pair[0], out key))
            {
                return false;
            }

            payloadNode = pair[1]?.DeepClone();
            return payloadNode is not null;
        }

        if (entry is JsonObject obj)
        {
            if (TryGetJsonPropertyIgnoreCase(obj, "name", out _, out JsonNode? nameNode) &&
                TryReadNonEmptyJsonString(nameNode, out string parsedName) &&
                TryGetJsonPropertyIgnoreCase(obj, "value", out _, out JsonNode? valueNode))
            {
                key = parsedName;
                payloadNode = valueNode?.DeepClone();
                return payloadNode is not null;
            }

            if (TryGetJsonPropertyIgnoreCase(obj, "key", out _, out JsonNode? keyNode) &&
                TryReadNonEmptyJsonString(keyNode, out string parsedKey) &&
                TryGetJsonPropertyIgnoreCase(obj, "value", out _, out JsonNode? keyedValueNode))
            {
                key = parsedKey;
                payloadNode = keyedValueNode?.DeepClone();
                return payloadNode is not null;
            }

            if (obj.Count == 1)
            {
                KeyValuePair<string, JsonNode?> single = obj.First();
                if (string.IsNullOrWhiteSpace(single.Key) || single.Value is null)
                {
                    return false;
                }

                key = single.Key;
                payloadNode = single.Value.DeepClone();
                return true;
            }
        }

        return false;
    }

    private static JsonNode NormalizePropertyPayloadForMap(JsonNode payloadNode, out bool changed)
    {
        changed = false;
        if (payloadNode is JsonObject payloadObj &&
            TryGetJsonPropertyIgnoreCase(payloadObj, "type", out _, out JsonNode? typeNode) &&
            TryGetJsonPropertyIgnoreCase(payloadObj, "value", out _, out JsonNode? valueNode))
        {
            if (!TryGetIntFromJsonNode(typeNode, out int parsedType))
            {
                return CanonicalizePropertyPayload(payloadNode, out changed);
            }

            int normalizedType = parsedType;
            JsonNode? normalizedValue;
            bool valueChanged = false;
            switch (parsedType)
            {
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                case 255:
                    normalizedValue = CoercePropertyValueForType(
                        ref normalizedType,
                        valueNode?.DeepClone(),
                        out valueChanged);
                    break;
                default:
                    normalizedValue = valueNode?.DeepClone();
                    break;
            }

            bool alreadyCanonical =
                payloadObj.Count == 2 &&
                payloadObj.ContainsKey("type") &&
                payloadObj.ContainsKey("value");
            bool typeChanged = normalizedType != parsedType;
            if (alreadyCanonical &&
                !typeChanged &&
                !valueChanged &&
                JsonNode.DeepEquals(payloadObj["value"], normalizedValue))
            {
                return payloadObj.DeepClone();
            }

            changed = true;
            return new JsonObject
            {
                ["type"] = JsonValue.Create(normalizedType),
                ["value"] = normalizedValue
            };
        }

        return CanonicalizePropertyPayload(payloadNode, out changed);
    }

    private static bool TryReadNonEmptyJsonString(JsonNode? node, out string value)
    {
        value = string.Empty;
        if (!TryReadJsonString(node, out string parsed) || string.IsNullOrWhiteSpace(parsed))
        {
            return false;
        }

        value = parsed;
        return true;
    }

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

    private static string BuildSingleLineExceptionPreview(Exception ex)
    {
        string message = ex.Message ?? string.Empty;
        message = message.Replace("\r", " ").Replace("\n", " ").Trim();
        if (message.Length <= 220)
        {
            return message;
        }

        return message[..217] + "...";
    }

    private static bool IsConnectionResetException(HttpRequestException ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socketException)
            {
                if (socketException.SocketErrorCode == SocketError.ConnectionReset ||
                    socketException.SocketErrorCode == SocketError.ConnectionAborted)
                {
                    return true;
                }
            }

            if (current is IOException ioException &&
                ioException.Message.Contains("closed", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsConnectionRefusedException(HttpRequestException ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            if (current is SocketException socketException &&
                socketException.SocketErrorCode == SocketError.ConnectionRefused)
            {
                return true;
            }
        }

        return false;
    }

    private static bool ShouldAttemptTransportRecovery(HttpRequestException ex)
    {
        return IsConnectionResetException(ex) || IsConnectionRefusedException(ex);
    }

    private static async Task WaitForEndpointPortRecoveryAsync(
        Uri endpoint,
        TimeSpan maxWait,
        CancellationToken cancellationToken)
    {
        DateTime startedAt = DateTime.UtcNow;
        while (DateTime.UtcNow - startedAt < maxWait)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await CanConnectTcpAsync(endpoint.Host, endpoint.Port, cancellationToken))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }
    }

    private static async Task<bool> CanConnectTcpAsync(string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            using var client = new TcpClient();
            Task connectTask = client.ConnectAsync(host, port);
            Task completed = await Task.WhenAny(connectTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));
            if (!ReferenceEquals(completed, connectTask))
            {
                return false;
            }

            await connectTask;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string BuildTransportErrorPreview(HttpRequestException ex)
    {
        string message = ex.Message;
        if (ex.InnerException is SocketException socketException)
        {
            return $"{message} (socket {(int)socketException.SocketErrorCode}: {socketException.SocketErrorCode})";
        }

        return message;
    }
}
