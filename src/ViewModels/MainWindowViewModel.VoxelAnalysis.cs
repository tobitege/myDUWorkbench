using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Media;
using myDUWorkbench.Models;
using myDUWorkbench.Services;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    public async Task<string> ExportLoadedConstructVoxelAnalysisJsonAsync(CancellationToken cancellationToken)
    {
        if (_lastSnapshot is not { } snapshot || snapshot.ConstructId == 0UL)
        {
            throw new InvalidOperationException("No loaded construct snapshot available.");
        }

        DataConnectionOptions? options = TryBuildNameLookupOptions();
        return await _dataService.ExportConstructVoxelAnalysisJsonAsync(
            snapshot.ConstructId,
            EndpointTemplateInput,
            BlueprintImportEndpointInput,
            options,
            cancellationToken);
    }

    public async Task<string> ExportBlueprintVoxelAnalysisFromJsonAsync(
        string jsonContent,
        string sourceName,
        CancellationToken cancellationToken)
    {
        DataConnectionOptions? options = TryBuildNameLookupOptions();
        return await _dataService.ExportBlueprintVoxelAnalysisJsonFromJsonContentAsync(
            jsonContent,
            sourceName,
            options,
            cancellationToken);
    }

    private DataConnectionOptions? TryBuildNameLookupOptions()
    {
        try
        {
            return BuildDbOptions();
        }
        catch
        {
            return null;
        }
    }
}
