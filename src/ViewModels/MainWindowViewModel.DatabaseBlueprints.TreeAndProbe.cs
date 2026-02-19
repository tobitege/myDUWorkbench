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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace myDUWorkbench.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    [RelayCommand]
    private void ExpandAllElementProperties()
    {
        ElementPropertiesModel.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllElementProperties()
    {
        ElementPropertiesModel.CollapseAll(minDepth: 0);
    }

    [RelayCommand]
    private void ApplyElementTypeNameFilter()
    {
        AddElementTypeFilterHistory(ElementTypeNameFilterInput);
        ApplyElementPropertyFilter();
    }

    [RelayCommand]
    private void ClearElementTypeFilterHistory()
    {
        ElementTypeNameFilterInput = string.Empty;
        SelectedElementTypeFilterHistoryItem = null;
        ApplyElementPropertyFilter();
        if (AutoCollapseToFirstLevel)
        {
            ElementPropertiesModel.CollapseAll(minDepth: 0);
        }
    }

    [RelayCommand]
    private void CheckAllElementPropertyFilters()
    {
        SetAllElementPropertyFilters(isActive: true);
    }

    [RelayCommand]
    private void UncheckAllElementPropertyFilters()
    {
        SetAllElementPropertyFilters(isActive: false);
    }

    [RelayCommand]
    private void ExpandAllLuaBlocks()
    {
        Dpuyaml6Model.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllLuaBlocks()
    {
        Dpuyaml6Model.CollapseAll(minDepth: 1);
    }

    [RelayCommand]
    private void ExpandAllHtmlRs()
    {
        Content2Model.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllHtmlRs()
    {
        Content2Model.CollapseAll(minDepth: 1);
    }

    [RelayCommand]
    private void ExpandAllDatabank()
    {
        DatabankModel.ExpandAll();
    }

    [RelayCommand]
    private void CollapseAllDatabank()
    {
        DatabankModel.CollapseAll(minDepth: 1);
    }

    [RelayCommand]
    private async Task ProbeEndpointAsync()
    {
        if (IsBusy)
        {
            return;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        try
        {
            IsBusy = true;
            StatusMessage = "Probing construct endpoint...";

            ulong constructId = ParseRequiredConstructId(ConstructIdInput);
            Uri uri = BuildEndpointUri(EndpointTemplateInput, constructId);

            EndpointProbeResult result = await _dataService.ProbeEndpointAsync(uri, cts.Token);
            _lastEndpointResult = result;
            UpdateEndpointSummary(result);

            StatusMessage = $"Endpoint probe finished with HTTP {result.StatusCode}.";
        }
        catch (Exception ex)
        {
            StatusMessage = $"Endpoint probe failed: {ex.Message}";
        }
        finally
        {
            IsBusy = false;
        }
    }

    public async Task RepairDestroyedElementsAsync(CancellationToken cancellationToken)
    {
        if (IsBusy || RepairInProgress)
        {
            return;
        }

        if (_lastSnapshot is null)
        {
            throw new InvalidOperationException("Load a DB snapshot before running repair.");
        }

        if (!IsDatabaseOnline())
        {
            throw new InvalidOperationException("DB is offline.");
        }

        try
        {
            IsBusy = true;
            RepairInProgress = true;
            RepairProgressPercent = 0d;
            RepairStatusText = "Repair: starting...";
            StatusMessage = "Repairing element state properties...";
            await Task.Yield();

            DataConnectionOptions options = BuildDbOptions();
            ulong constructId = _lastSnapshot.ConstructId;

            var progress = new Progress<DestroyedRepairProgress>(state =>
            {
                if (state.TotalCount <= 0)
                {
                    RepairProgressPercent = 0d;
                    RepairStatusText = "Repair: no matching properties found.";
                    return;
                }

                RepairProgressPercent = state.ProcessedCount * 100d / state.TotalCount;
                RepairStatusText = $"Repair: {state.ProcessedCount}/{state.TotalCount}";
            });

            DestroyedRepairResult result = await _dataService.RepairDestroyedPropertiesAsync(
                options,
                constructId,
                progress,
                cancellationToken);

            ApplyRepairToLoadedSnapshot();

            if (result.TotalCount == 0)
            {
                RepairProgressPercent = 0d;
                RepairStatusText = "Repair: no matching properties found.";
                StatusMessage = "Repair finished: no destroyed/restoreCount properties found.";
                return;
            }

            RepairProgressPercent = 100d;
            RepairStatusText = $"Repair complete: {result.UpdatedCount}/{result.TotalCount}";
            StatusMessage = $"Repair finished: removed {result.UpdatedCount} destroyed/restoreCount row(s).";
        }
        finally
        {
            RepairInProgress = false;
            IsBusy = false;
        }
    }
}
