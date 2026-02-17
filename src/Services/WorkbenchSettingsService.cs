// Helper Index:
// - Load: Restores persisted workbench settings and falls back to defaults on read/parse failure.
// - SaveAsync: Serializes and writes settings with save-gate synchronization.
// - BuildPersistedSettings: Maps runtime settings into persisted DTO form.
// - EncryptPassword / DecryptPassword: Protects DB credentials with Windows DPAPI user scope.
using myDUWorker.Models;
using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorker.Services;

public sealed class WorkbenchSettingsService
{
    private static readonly byte[] PasswordEntropy = Encoding.UTF8.GetBytes("mydu-workbench-db-password-v1");
    private readonly string _settingsFilePath;
    private readonly SemaphoreSlim _saveGate = new(1, 1);

    public WorkbenchSettingsService(string? settingsFilePath = null)
    {
        _settingsFilePath = settingsFilePath ??
            Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "myDUWorker",
                "workbench-settings.json");
    }

    public WorkbenchSettings Load()
    {
        try
        {
            if (!File.Exists(_settingsFilePath))
            {
                return new WorkbenchSettings();
            }

            string json = File.ReadAllText(_settingsFilePath);
            PersistedWorkbenchSettings persisted = JsonSerializer.Deserialize<PersistedWorkbenchSettings>(json) ?? new PersistedWorkbenchSettings();
            return new WorkbenchSettings
            {
                ConstructIdInput = persisted.ConstructIdInput,
                PlayerIdInput = persisted.PlayerIdInput,
                ConstructNameSearchInput = persisted.ConstructNameSearchInput,
                PlayerNameSearchInput = persisted.PlayerNameSearchInput,
                EndpointTemplateInput = persisted.EndpointTemplateInput,
                DbHostInput = persisted.DbHostInput,
                ServerRootPathInput = persisted.ServerRootPathInput,
                DbPortInput = persisted.DbPortInput,
                DbNameInput = persisted.DbNameInput,
                DbUserInput = persisted.DbUserInput,
                DbPassword = DecryptPassword(persisted.EncryptedDbPassword),
                PropertyLimitInput = persisted.PropertyLimitInput,
                ElementTypeNameFilterInput = persisted.ElementTypeNameFilterInput,
                ElementTypeFilterHistory = persisted.ElementTypeFilterHistory ?? new(),
                AutoLoadOnStartup = persisted.AutoLoadOnStartup,
                AutoLoadPlayerNames = persisted.AutoLoadPlayerNames,
                LimitToSelectedPlayerConstructs = persisted.LimitToSelectedPlayerConstructs,
                AutoConnectDatabase = persisted.AutoConnectDatabase,
                AutoConnectRetrySeconds = persisted.AutoConnectRetrySeconds,
                AutoWrapContent = persisted.AutoWrapContent,
                AutoCollapseToFirstLevel = persisted.AutoCollapseToFirstLevel,
                LuaVersioningEnabled = persisted.LuaVersioningEnabled,
                LastSavedFolder = persisted.LastSavedFolder,
                SelectedConstructSuggestionId = persisted.SelectedConstructSuggestionId,
                SelectedConstructSuggestionName = persisted.SelectedConstructSuggestionName,
                SelectedElementNodeKey = persisted.SelectedElementNodeKey,
                SelectedDpuyamlNodeKey = persisted.SelectedDpuyamlNodeKey,
                SelectedContent2NodeKey = persisted.SelectedContent2NodeKey,
                SelectedDatabankNodeKey = persisted.SelectedDatabankNodeKey,
                GridColumnWidths = persisted.GridColumnWidths ?? new(),
                ElementPropertyActiveStates = persisted.ElementPropertyActiveStates ?? new(),
                WindowPlacement = persisted.WindowPlacement ?? new()
            };
        }
        catch
        {
            return new WorkbenchSettings();
        }
    }

    public async Task SaveAsync(WorkbenchSettings settings, CancellationToken cancellationToken)
    {
        await _saveGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            string? directory = Path.GetDirectoryName(_settingsFilePath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            PersistedWorkbenchSettings persisted = await Task.Run(
                () => BuildPersistedSettings(settings),
                cancellationToken).ConfigureAwait(false);

            string json = await Task.Run(
                () => JsonSerializer.Serialize(persisted, new JsonSerializerOptions
                {
                    WriteIndented = true
                }),
                cancellationToken).ConfigureAwait(false);

            await File.WriteAllTextAsync(_settingsFilePath, json, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _saveGate.Release();
        }
    }

    private static PersistedWorkbenchSettings BuildPersistedSettings(WorkbenchSettings settings)
    {
        return new PersistedWorkbenchSettings
        {
            ConstructIdInput = settings.ConstructIdInput,
            PlayerIdInput = settings.PlayerIdInput,
            ConstructNameSearchInput = settings.ConstructNameSearchInput,
            PlayerNameSearchInput = settings.PlayerNameSearchInput,
            EndpointTemplateInput = settings.EndpointTemplateInput,
            DbHostInput = settings.DbHostInput,
            ServerRootPathInput = settings.ServerRootPathInput,
            DbPortInput = settings.DbPortInput,
            DbNameInput = settings.DbNameInput,
            DbUserInput = settings.DbUserInput,
            EncryptedDbPassword = EncryptPassword(settings.DbPassword),
            PropertyLimitInput = settings.PropertyLimitInput,
            ElementTypeNameFilterInput = settings.ElementTypeNameFilterInput,
            ElementTypeFilterHistory = settings.ElementTypeFilterHistory ?? new(),
            AutoLoadOnStartup = settings.AutoLoadOnStartup,
            AutoLoadPlayerNames = settings.AutoLoadPlayerNames,
            LimitToSelectedPlayerConstructs = settings.LimitToSelectedPlayerConstructs,
            AutoConnectDatabase = settings.AutoConnectDatabase,
            AutoConnectRetrySeconds = settings.AutoConnectRetrySeconds,
            AutoWrapContent = settings.AutoWrapContent,
            AutoCollapseToFirstLevel = settings.AutoCollapseToFirstLevel,
            LuaVersioningEnabled = settings.LuaVersioningEnabled,
            LastSavedFolder = settings.LastSavedFolder,
            SelectedConstructSuggestionId = settings.SelectedConstructSuggestionId,
            SelectedConstructSuggestionName = settings.SelectedConstructSuggestionName,
            SelectedElementNodeKey = settings.SelectedElementNodeKey,
            SelectedDpuyamlNodeKey = settings.SelectedDpuyamlNodeKey,
            SelectedContent2NodeKey = settings.SelectedContent2NodeKey,
            SelectedDatabankNodeKey = settings.SelectedDatabankNodeKey,
            GridColumnWidths = settings.GridColumnWidths ?? new(),
            ElementPropertyActiveStates = settings.ElementPropertyActiveStates ?? new(),
            WindowPlacement = settings.WindowPlacement ?? new()
        };
    }

    private static string EncryptPassword(string plainPassword)
    {
        if (string.IsNullOrEmpty(plainPassword))
        {
            return string.Empty;
        }

        if (!OperatingSystem.IsWindows())
        {
            return string.Empty;
        }

        byte[] plaintext = Encoding.UTF8.GetBytes(plainPassword);
        byte[] protectedBytes = ProtectedData.Protect(plaintext, PasswordEntropy, DataProtectionScope.CurrentUser);
        return Convert.ToBase64String(protectedBytes);
    }

    private static string DecryptPassword(string encryptedPassword)
    {
        if (string.IsNullOrWhiteSpace(encryptedPassword))
        {
            return string.Empty;
        }

        if (!OperatingSystem.IsWindows())
        {
            return string.Empty;
        }

        try
        {
            byte[] protectedBytes = Convert.FromBase64String(encryptedPassword);
            byte[] plainBytes = ProtectedData.Unprotect(protectedBytes, PasswordEntropy, DataProtectionScope.CurrentUser);
            return Encoding.UTF8.GetString(plainBytes);
        }
        catch
        {
            return string.Empty;
        }
    }
}
