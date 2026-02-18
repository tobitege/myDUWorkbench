using Avalonia.Controls;
using Avalonia.Interactivity;
using System;

namespace myDUWorker.Views;

public partial class TextInputDialog : Window
{
    private readonly Func<string, string?>? _validateInput;

    public TextInputDialog()
    {
        InitializeComponent();
    }

    public TextInputDialog(
        string title,
        string prompt,
        string initialValue = "",
        string confirmButtonText = "Confirm",
        string cancelButtonText = "Cancel",
        Func<string, string?>? validateInput = null)
        : this()
    {
        Title = string.IsNullOrWhiteSpace(title) ? "Input" : title.Trim();
        PromptText.Text = string.IsNullOrWhiteSpace(prompt) ? "Input" : prompt.Trim();
        InputTextBox.Text = initialValue ?? string.Empty;
        ConfirmButton.Content = string.IsNullOrWhiteSpace(confirmButtonText) ? "Confirm" : confirmButtonText.Trim();
        CancelButton.Content = string.IsNullOrWhiteSpace(cancelButtonText) ? "Cancel" : cancelButtonText.Trim();
        _validateInput = validateInput;
    }

    protected override void OnOpened(EventArgs e)
    {
        base.OnOpened(e);
        InputTextBox.Focus();
        InputTextBox.CaretIndex = InputTextBox.Text?.Length ?? 0;
    }

    private void OnConfirmClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;

        string value = InputTextBox.Text ?? string.Empty;
        string? validationError = _validateInput?.Invoke(value);
        if (!string.IsNullOrWhiteSpace(validationError))
        {
            ValidationText.Text = validationError.Trim();
            ValidationText.IsVisible = true;
            return;
        }

        ValidationText.Text = string.Empty;
        ValidationText.IsVisible = false;
        Close(value);
    }

    private void OnCancelClick(object? sender, RoutedEventArgs e)
    {
        _ = sender;
        _ = e;
        Close(null);
    }
}
