using System;
using Avalonia;
using Avalonia.Media;
using AvaloniaEdit;
using AvaloniaEdit.Rendering;

namespace myDUWorker.Helpers;

public sealed class ExecutionLineHighlighter : IBackgroundRenderer
{
    private static readonly IBrush CurrentFileBrush = new SolidColorBrush(Color.FromArgb(60, 255, 255, 0));
    private static readonly IBrush OtherFileBrush = new SolidColorBrush(Color.FromArgb(60, 255, 0, 0));

    private readonly TextEditor _editor;

    public ExecutionLineHighlighter(TextEditor editor)
    {
        _editor = editor ?? throw new ArgumentNullException(nameof(editor));
    }

    public int ExecutionLine { get; set; } = -1;

    public bool IsCurrentFile { get; set; } = true;

    public KnownLayer Layer => KnownLayer.Background;

    public void Draw(TextView textView, DrawingContext drawingContext)
    {
        if (ExecutionLine <= 0 || ExecutionLine > _editor.Document.LineCount)
        {
            return;
        }

        if (textView.VisualLines.Count == 0)
        {
            return;
        }

        foreach (VisualLine visualLine in textView.VisualLines)
        {
            if (visualLine.FirstDocumentLine.LineNumber != ExecutionLine)
            {
                continue;
            }

            IBrush brush = IsCurrentFile ? CurrentFileBrush : OtherFileBrush;
            double top = visualLine.GetTextLineVisualYPosition(visualLine.TextLines[0], VisualYPosition.LineTop) - textView.ScrollOffset.Y;
            double bottom = visualLine.GetTextLineVisualYPosition(visualLine.TextLines[0], VisualYPosition.LineBottom) - textView.ScrollOffset.Y;
            drawingContext.FillRectangle(brush, new Rect(0, top, textView.Bounds.Width, bottom - top));
            break;
        }
    }
}
