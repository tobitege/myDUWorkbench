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
}
