using Avalonia.Controls;
using Avalonia.Interactivity;

namespace myDUWorkbench.Views;

public partial class BlueprintExportOptionsDialog : Window
{
    public BlueprintExportOptionsDialog()
    {
        InitializeComponent();
    }

    public bool ExcludeVoxels => ExcludeVoxelsCheckBox.IsChecked == true;
    public bool ExcludeElementsLinks => ExcludeElementsLinksCheckBox.IsChecked == true;

    private void OnExportClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(true);
    }

    private void OnCancelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(false);
    }
}
