// Helper Index:
// - PropertyFilterRecord: Binds a property name to an observable active/inactive filter flag.
using CommunityToolkit.Mvvm.ComponentModel;

namespace myDUWorkbench.Models;

public partial class PropertyFilterRecord : ObservableObject
{
    public PropertyFilterRecord(string propertyName, bool isActive)
    {
        PropertyName = propertyName;
        this.isActive = isActive;
    }

    public string PropertyName { get; }

    [ObservableProperty]
    private bool isActive;
}
