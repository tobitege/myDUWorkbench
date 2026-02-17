// Helper Index:
// - LuaBackupCreateRequest: Captures editor/script metadata needed to create versioned backup snapshots.
// - LuaBackupEntry: Metadata row used by backup manager grid.
// - LuaBackupDocument: Loaded backup payload and parsed script content.
namespace myDUWorker.Models;

using System;

public sealed record LuaBackupCreateRequest(
    string Content,
    string SuggestedFileName,
    string SourceFilePath,
    ulong? ElementId,
    string ElementDisplayName,
    string NodeLabel,
    string PropertyName);

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
    string Preview);

public sealed record LuaBackupDocument(
    LuaBackupEntry Entry,
    string RawFileContent,
    string ScriptContent);
