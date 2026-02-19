using Avalonia.Controls;
using Avalonia.Interactivity;

namespace myDUWorkbench.Views;

public partial class ConfirmationDialog : Window
{
    public ConfirmationDialog()
    {
        InitializeComponent();
    }

    public ConfirmationDialog(
        string title,
        string prompt,
        string confirmButtonText = "Confirm",
        string cancelButtonText = "Cancel",
        string? hintText = null,
        string? detailsText = null,
        string? option1Label = null,
        bool option1Checked = false,
        string? option2Label = null,
        bool option2Checked = false,
        string? option3Label = null,
        bool option3Checked = false,
        string? option4Label = null,
        bool option4Checked = false)
        : this()
    {
        string resolvedTitle = string.IsNullOrWhiteSpace(title) ? "Confirm action" : title.Trim();
        string resolvedPrompt = string.IsNullOrWhiteSpace(prompt) ? "Are you sure?" : prompt.Trim();
        string resolvedConfirmText = string.IsNullOrWhiteSpace(confirmButtonText) ? "Confirm" : confirmButtonText.Trim();
        string resolvedCancelText = string.IsNullOrWhiteSpace(cancelButtonText) ? "Cancel" : cancelButtonText.Trim();

        Title = resolvedTitle;
        PromptText.Text = resolvedPrompt;
        ConfirmButton.Content = resolvedConfirmText;
        CancelButton.Content = resolvedCancelText;

        if (!string.IsNullOrWhiteSpace(detailsText))
        {
            DetailsText.Text = detailsText.Trim();
            DetailsText.IsVisible = true;
        }

        if (!string.IsNullOrWhiteSpace(hintText))
        {
            HintText.Text = hintText.Trim();
            HintBorder.IsVisible = true;
        }

        bool hasOptions = false;

        if (!string.IsNullOrWhiteSpace(option1Label))
        {
            Option1CheckBox.Content = option1Label.Trim();
            Option1CheckBox.IsChecked = option1Checked;
            Option1CheckBox.IsVisible = true;
            hasOptions = true;
        }

        if (!string.IsNullOrWhiteSpace(option2Label))
        {
            Option2CheckBox.Content = option2Label.Trim();
            Option2CheckBox.IsChecked = option2Checked;
            Option2CheckBox.IsVisible = true;
            hasOptions = true;
        }

        if (!string.IsNullOrWhiteSpace(option3Label))
        {
            Option3CheckBox.Content = option3Label.Trim();
            Option3CheckBox.IsChecked = option3Checked;
            Option3CheckBox.IsVisible = true;
            hasOptions = true;
        }

        if (!string.IsNullOrWhiteSpace(option4Label))
        {
            Option4CheckBox.Content = option4Label.Trim();
            Option4CheckBox.IsChecked = option4Checked;
            Option4CheckBox.IsVisible = true;
            hasOptions = true;
        }

        OptionsPanel.IsVisible = hasOptions;
    }

    public bool IsOption1Checked => Option1CheckBox.IsChecked == true;
    public bool IsOption2Checked => Option2CheckBox.IsChecked == true;
    public bool IsOption3Checked => Option3CheckBox.IsChecked == true;
    public bool IsOption4Checked => Option4CheckBox.IsChecked == true;

    private void OnConfirmClick(object? sender, RoutedEventArgs e)
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
