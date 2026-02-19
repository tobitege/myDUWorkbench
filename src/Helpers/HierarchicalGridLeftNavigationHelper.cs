using System;
using Avalonia.Controls;
using Avalonia.Controls.DataGridHierarchical;
using Avalonia.Input;

namespace myDUWorkbench.Helpers;

public static class HierarchicalGridLeftNavigationHelper
{
    public static void Attach(DataGrid grid)
    {
        if (grid is null)
        {
            throw new ArgumentNullException(nameof(grid));
        }

        Detach(grid);
        grid.KeyDown += OnGridKeyDown;
    }

    public static void Detach(DataGrid grid)
    {
        if (grid is null)
        {
            return;
        }

        grid.KeyDown -= OnGridKeyDown;
    }

    private static void OnGridKeyDown(object? sender, KeyEventArgs e)
    {
        if (sender is not DataGrid grid ||
            e.Handled ||
            e.Key != Key.Left ||
            e.KeyModifiers != KeyModifiers.None)
        {
            return;
        }

        if (TryHandleLeftKey(grid))
        {
            e.Handled = true;
        }
    }

    private static bool TryHandleLeftKey(DataGrid grid)
    {
        if (grid.HierarchicalModel is not IHierarchicalModel model)
        {
            return false;
        }

        HierarchicalNode? selectedNode = ResolveSelectedNode(grid.SelectedItem, model);
        if (selectedNode is null || selectedNode == model.Root)
        {
            return false;
        }

        if (!selectedNode.IsLeaf && selectedNode.IsExpanded)
        {
            model.Collapse(selectedNode);
            return true;
        }

        HierarchicalNode? parent = selectedNode.Parent;
        if (parent is null || parent == model.Root)
        {
            return false;
        }

        grid.SelectedItem = parent;
        return true;
    }

    private static HierarchicalNode? ResolveSelectedNode(object? selectedItem, IHierarchicalModel model)
    {
        if (selectedItem is HierarchicalNode selectedNode)
        {
            return selectedNode;
        }

        if (selectedItem is null)
        {
            return null;
        }

        return model.FindNode(selectedItem);
    }
}
