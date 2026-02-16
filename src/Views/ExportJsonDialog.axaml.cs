// Helper Index:
// - ExportJsonDialog(string json): Initializes dialog with prefilled export payload text.
// - OnCloseClick: Closes the modal dialog from the UI button handler.
using Avalonia.Controls;
using Avalonia.Interactivity;

namespace MyDu.Views;

public partial class ExportJsonDialog : Window
{
    public ExportJsonDialog()
    {
        InitializeComponent();
    }

    public ExportJsonDialog(string json)
        : this()
    {
        JsonTextBox.Text = json;
    }

    private void OnCloseClick(object? sender, RoutedEventArgs e)
    {
        Close();
    }
}
