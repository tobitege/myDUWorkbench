using Avalonia.Controls;
using Avalonia.Interactivity;

namespace myDUWorkbench.Views;

public enum LuaPersistenceCleanupAction
{
    Cancel = 0,
    RemoveInvalidCharacters = 1,
    ReplaceInvalidCharactersWithBlanks = 2
}

public partial class LuaPersistenceCleanupDialog : Window
{
    public LuaPersistenceCleanupDialog()
    {
        InitializeComponent();
    }

    public LuaPersistenceCleanupDialog(string actionDescription, int nullCount, int unpairedSurrogateCount)
        : this()
    {
        string action = string.IsNullOrWhiteSpace(actionDescription) ? "save" : actionDescription.Trim();
        int total = nullCount + unpairedSurrogateCount;
        IssueSummaryText.Text =
            $"Detected {total} invalid character(s): NUL=\\0 ({nullCount}), unpaired surrogates ({unpairedSurrogateCount}).";
        ActionSummaryText.Text =
            $"Choose how to {action}: remove invalid characters, replace them with blanks, or cancel.";
    }

    private void OnRemoveInvalidClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(LuaPersistenceCleanupAction.RemoveInvalidCharacters);
    }

    private void OnReplaceInvalidClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(LuaPersistenceCleanupAction.ReplaceInvalidCharactersWithBlanks);
    }

    private void OnCancelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(LuaPersistenceCleanupAction.Cancel);
    }
}
