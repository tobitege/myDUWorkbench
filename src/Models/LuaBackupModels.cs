// Helper Index:
// - LuaBackupCreateRequest: Captures editor/script metadata needed to create versioned backup snapshots.
// - LuaBackupEntry: Metadata row used by backup manager grid.
// - LuaBackupDocument: Loaded backup payload and parsed script content.
namespace myDUWorkbench.Models;

using System;

public enum BackupContentKind
{
    Lua = 0,
    Databank = 1
}

public sealed record LuaBackupCreateRequest(
    string Content,
    string SuggestedFileName,
    string SourceFilePath,
    ulong? ElementId,
    string ElementDisplayName,
    string NodeLabel,
    string PropertyName,
    BackupContentKind ContentKind = BackupContentKind.Lua);

public sealed record LuaBackupEntry(
    string FilePath,
    string FileName,
    DateTimeOffset BackupUtc,
    ulong? ElementId,
    string ElementDisplayName,
    string NodeLabel,
    string PropertyName,
    string SourceFilePath,
    string SuggestedFileName,
    string Preview,
    BackupContentKind ContentKind = BackupContentKind.Lua);

public sealed record LuaBackupDocument(
    LuaBackupEntry Entry,
    string RawFileContent,
    string ScriptContent);

public sealed record BackupManagerDialogResult(
    string Content,
    BackupContentKind ContentKind,
    ulong? ElementId,
    string ElementDisplayName,
    string NodeLabel,
    string PropertyName,
    string SuggestedFileName);
