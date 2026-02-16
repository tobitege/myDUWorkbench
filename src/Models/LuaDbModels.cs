// Helper Index:
// - LuaPropertyRawRecord: Raw element_property row data used for optimistic DB updates.
// - LuaDbSaveResult: Save outcome metadata used to refresh editor DB baseline state.
namespace MyDu.Models;

public sealed record LuaPropertyRawRecord(
    ulong ElementId,
    string PropertyName,
    int PropertyType,
    byte[] RawValue);

public sealed record LuaDbSaveResult(
    byte[] NewDbValue,
    int PropertyType,
    bool UsesHashReference,
    string? HashReference,
    int SectionCount);
