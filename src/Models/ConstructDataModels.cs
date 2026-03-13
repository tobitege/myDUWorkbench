// Helper Index:
// - PropertyTreeRow.ElementTypeName: Normalizes element labels by stripping trailing local-id suffixes.
// - ConstructNameLookupRecord.DisplayLabel: Provides stable "id | name" construct suggestion text.
// - PlayerNameLookupRecord.DisplayLabel: Provides stable "id | name" player suggestion text.
// - UserConstructRecord.DisplayLabel: Summarizes construct identity, core kind, and owner context.
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace myDUWorkbench.Models;

public sealed record DataConnectionOptions(
    string ServerRootPath,
    string Host,
    int Port,
    string Database,
    string Username,
    string Password);

public sealed record ElementPropertyRecord(
    ulong ElementId,
    string ElementDisplayName,
    string Name,
    int PropertyType,
    string DecodedValue,
    int ByteLength);

public sealed record DestroyedRepairProgress(
    int ProcessedCount,
    int TotalCount);

public sealed record DestroyedRepairResult(
    int TotalCount,
    int UpdatedCount);

public sealed class PropertyTreeRow
{
    public PropertyTreeRow(
        string nodeLabel,
        string nodeKind,
        ulong? elementId,
        string elementDisplayName,
        string propertyName,
        int? propertyType,
        int? byteLength,
        string valuePreview,
        string fullContent,
        string elementName = "")
    {
        NodeLabel = nodeLabel;
        NodeKind = nodeKind;
        ElementId = elementId;
        ElementDisplayName = elementDisplayName;
        PropertyName = propertyName;
        PropertyType = propertyType;
        ByteLength = byteLength;
        ValuePreview = valuePreview;
        FullContent = fullContent;
        ElementName = elementName ?? string.Empty;
    }

    public string NodeLabel { get; }
    public string NodeKind { get; }
    public ulong? ElementId { get; }
    public string ElementDisplayName { get; }
    public string PropertyName { get; }
    public int? PropertyType { get; }
    public int? ByteLength { get; }
    public string ValuePreview { get; }
    public string FullContent { get; }
    public string ElementName { get; }
    public ObservableCollection<PropertyTreeRow> Children { get; } = new();
    public string ElementTypeName => StripTrailingLocalId(ElementDisplayName);

    public string ElementIdText => ElementId.HasValue ? ElementId.Value.ToString() : string.Empty;
    public string PropertyTypeText => PropertyType.HasValue ? PropertyType.Value.ToString() : string.Empty;
    public string ByteLengthText => ByteLength.HasValue ? ByteLength.Value.ToString() : string.Empty;

    private static string StripTrailingLocalId(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        int bracketStart = value.LastIndexOf(" [", StringComparison.Ordinal);
        if (bracketStart < 0 || !value.EndsWith("]", StringComparison.Ordinal))
        {
            return value;
        }

        int idStart = bracketStart + 2;
        int idLength = value.Length - idStart - 1;
        if (idLength <= 0)
        {
            return value;
        }

        ReadOnlySpan<char> idSpan = value.AsSpan(idStart, idLength);
        for (int i = 0; i < idSpan.Length; i++)
        {
            if (!char.IsDigit(idSpan[i]))
            {
                return NormalizeElementTypeValue(value);
            }
        }

        return NormalizeElementTypeValue(value[..bracketStart]);
    }

    private static string NormalizeElementTypeValue(string value)
    {
        if (string.Equals(value, "BlueprintElement", StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        return value;
    }
}

public sealed record ConstructNameLookupRecord(
    ulong ConstructId,
    string ConstructName)
{
    public string DisplayLabel => $"{ConstructId} | {ConstructName}";
}

public sealed record PlayerNameLookupRecord(
    ulong? PlayerId,
    string PlayerName)
{
    public string DisplayLabel => PlayerId.HasValue ? $"{PlayerId.Value} | {PlayerName}" : PlayerName;
}

public sealed record DatabaseConstructSnapshot(
    ulong ConstructId,
    string ConstructName,
    Vec3 Position,
    Quat Rotation,
    ulong? PlayerId,
    string? PlayerName,
    ulong? PlayerConstructId,
    double? ConstructMass,
    double? CurrentMass,
    double? SpeedFactor,
    Vec3? ResumeLinearVelocity,
    Vec3? ResumeAngularVelocity,
    IReadOnlyList<ElementPropertyRecord> Properties);

public sealed record BlueprintImportResult(
    string SourceName,
    string BlueprintName,
    ulong? BlueprintId,
    int ElementCount,
    IReadOnlyList<ElementPropertyRecord> Properties,
    string ImportPipeline,
    string ImportNotes);

public sealed record BlueprintGameDatabaseImportResult(
    Uri Endpoint,
    int StatusCode,
    ulong? BlueprintId,
    string ResponseText,
    string RequestNotes);

public sealed record EndpointProbeResult(
    Uri Url,
    int StatusCode,
    string ContentType,
    int PayloadSize,
    ConstructUpdate? ConstructUpdate,
    ConstructInfoPreamble? ConstructInfoPreamble,
    NqStructBlobHeader? BlobHeader,
    string RawPreview,
    string Notes);

public enum ConstructCoreKind
{
    Dynamic = 0,
    Static = 1,
    Space = 2,
    Unknown = 3
}

public enum ConstructListSort
{
    Name = 0,
    Id = 1
}

public sealed record UserConstructRecord(
    ulong ConstructId,
    string ConstructName,
    ulong? OwnerPlayerId,
    ulong? OwnerOrganizationId,
    ConstructCoreKind CoreKind,
    ulong? CoreElementId,
    ulong? CoreElementTypeId,
    string CoreElementDisplayName)
{
    public string DisplayLabel =>
        $"{ConstructId} | {ConstructName} | core={CoreKind} | ownerP={OwnerPlayerId?.ToString() ?? "-"} | ownerO={OwnerOrganizationId?.ToString() ?? "-"}";
}

public sealed record BlueprintDbRecord(
    ulong Id,
    string Name,
    ulong? CreatorId,
    DateTime? CreatedAt,
    bool FreeDeploy,
    long? MaxUse,
    bool HasMaterials,
    int ElementCount)
{
    public string CreatedAtDisplay => CreatedAt?.ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    public string MaxUseDisplay => MaxUse switch
    {
        null => "Core",
        1 => "Single-use",
        0 => "Expired (0)",
        long value => value.ToString(System.Globalization.CultureInfo.InvariantCulture)
    };
    public string CreatorIdDisplay => CreatorId?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "-";
}

public sealed record ElementTypeCountRecord(
    ulong ElementTypeId,
    string DisplayName,
    long Quantity);

public sealed record BlueprintDeleteResult(
    ulong BlueprintId,
    int BlueprintRowsDeleted,
    int ElementRowsDeleted,
    int ElementPropertyRowsDeleted,
    bool VoxelCleanupAttempted,
    bool VoxelCleanupSucceeded,
    string VoxelCleanupNote);

public sealed record BlueprintDeleteProgress(
    int CurrentIndex,
    int TotalCount,
    ulong BlueprintId,
    string BlueprintName);

public sealed record BlueprintUpdateResult(
    ulong BlueprintId,
    int RowsUpdated);

public sealed record BlueprintCopyResult(
    ulong SourceBlueprintId,
    ulong? CopiedBlueprintId,
    int BlueprintRowsInserted,
    int ElementRowsCopied,
    int ElementPropertyRowsCopied,
    string CopyNote);

public sealed record BlueprintGrantResult(
    ulong BlueprintId,
    ulong PlayerId,
    bool SingleUse,
    ulong ItemTypeId,
    short SlotNumber,
    int InventoryRowsInserted,
    bool AlreadyPresent,
    string Note);
