using Avalonia.Controls;
using Avalonia.Interactivity;

namespace myDUWorker.Views;

public partial class ElementTypeSummaryExportDialog : Window
{
    private readonly bool _allowSelectedRows;

    public ElementTypeSummaryExportDialog()
    {
        InitializeComponent();
    }

    public ElementTypeSummaryExportDialog(string sourceLabel, int selectedRowCount)
        : this()
    {
        string sourceText = string.IsNullOrWhiteSpace(sourceLabel) ? "Element summary" : sourceLabel.Trim();
        Title = $"{sourceText} Export Options";
        HeaderTextBlock.Text = $"{sourceText} export";
        DescriptionTextBlock.Text = "Export distinct element counts grouped by element_type_id.";

        _allowSelectedRows = selectedRowCount > 1;
        if (_allowSelectedRows)
        {
            SelectedRowsRadio.Content = $"Only selected rows ({selectedRowCount})";
            SelectedRowsRadio.IsVisible = true;
            SelectedRowsRadio.IsEnabled = true;
            RowScopeHintText.IsVisible = false;
        }
        else
        {
            SelectedRowsRadio.IsVisible = false;
            SelectedRowsRadio.IsEnabled = false;
            RowScopeHintText.IsVisible = true;
            AllRowsRadio.IsChecked = true;
        }
    }

    public bool UseDisplayNameField => NameFieldRadio.IsChecked == true;
    public bool UseSelectedRowsOnly => _allowSelectedRows && SelectedRowsRadio.IsChecked == true;

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
