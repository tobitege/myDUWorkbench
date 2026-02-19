using System;
using System.Collections.Generic;
using System.Linq;
using Avalonia;
using Avalonia.Input;
using Avalonia.Media;
using AvaloniaEdit.Editing;
using AvaloniaEdit.Rendering;

namespace myDUWorkbench.Controls;

public sealed class BreakpointMargin : AbstractMargin
{
    private const double DefaultMarginWidth = 28;

    private static readonly IBrush BreakpointFill = new SolidColorBrush(Color.FromRgb(178, 34, 34));
    private static readonly IPen BreakpointPen = new Pen(new SolidColorBrush(Color.FromRgb(128, 0, 0)), 1);
    private static readonly IBrush ExecutionFillCurrent = new SolidColorBrush(Color.FromRgb(218, 165, 32));
    private static readonly IBrush ExecutionFillOther = new SolidColorBrush(Color.FromRgb(178, 34, 34));
    private static readonly IPen ExecutionPenCurrent = new Pen(new SolidColorBrush(Color.FromRgb(184, 134, 11)), 1);
    private static readonly IPen ExecutionPenOther = new Pen(new SolidColorBrush(Color.FromRgb(139, 0, 0)), 1);

    private double _marginWidth = DefaultMarginWidth;
    private IBrush _backgroundBrush = Brushes.Transparent;

    public IReadOnlyCollection<int> Breakpoints { get; set; } = Array.Empty<int>();

    public int ExecutionLine { get; set; } = -1;

    public bool ExecutionLineIsCurrentFile { get; set; } = true;

    public double MarginWidth
    {
        get => _marginWidth;
        set
        {
            double normalized = Math.Clamp(value, 16d, 120d);
            if (Math.Abs(_marginWidth - normalized) < 0.01d)
            {
                return;
            }

            _marginWidth = normalized;
            InvalidateMeasure();
            InvalidateVisual();
        }
    }

    public IBrush BackgroundBrush
    {
        get => _backgroundBrush;
        set
        {
            IBrush resolved = value ?? Brushes.Transparent;
            if (ReferenceEquals(_backgroundBrush, resolved))
            {
                return;
            }

            _backgroundBrush = resolved;
            InvalidateVisual();
        }
    }

    public event EventHandler<int>? MarginClicked;

    protected override Size MeasureOverride(Size availableSize)
    {
        return new Size(MarginWidth, 0);
    }

    public override void Render(DrawingContext drawingContext)
    {
        TextView? textView = TextView;
        if (textView is null || !textView.VisualLinesValid)
        {
            return;
        }

        drawingContext.FillRectangle(BackgroundBrush, new Rect(0, 0, MarginWidth, Bounds.Height));

        double markerX = Math.Clamp(MarginWidth * 0.4, 8d, MarginWidth - 8d);
        double arrowBaseX = Math.Clamp(MarginWidth - 8d, markerX + 3d, MarginWidth - 3d);
        double arrowTipX = Math.Clamp(MarginWidth - 2d, arrowBaseX + 2d, MarginWidth - 1d);

        foreach (int lineNumber in Breakpoints.Distinct())
        {
            if (!TryGetLineCenterY(lineNumber, out double y))
            {
                continue;
            }

            drawingContext.DrawEllipse(BreakpointFill, BreakpointPen, new Point(markerX, y), 5, 5);
        }

        if (ExecutionLine > 0 && TryGetLineCenterY(ExecutionLine, out double executionY))
        {
            IBrush fill = ExecutionLineIsCurrentFile ? ExecutionFillCurrent : ExecutionFillOther;
            IPen pen = ExecutionLineIsCurrentFile ? ExecutionPenCurrent : ExecutionPenOther;
            var geometry = new StreamGeometry();

            using (StreamGeometryContext context = geometry.Open())
            {
                context.BeginFigure(new Point(arrowBaseX, executionY - 5), true);
                context.LineTo(new Point(arrowTipX, executionY));
                context.LineTo(new Point(arrowBaseX, executionY + 5));
                context.EndFigure(true);
            }

            drawingContext.DrawGeometry(fill, pen, geometry);
        }
    }

    protected override void OnPointerPressed(PointerPressedEventArgs e)
    {
        base.OnPointerPressed(e);

        TextView? textView = TextView;
        if (textView is null)
        {
            return;
        }

        Point pos = e.GetPosition(textView);
        VisualLine? visualLine = textView.GetVisualLineFromVisualTop(pos.Y + textView.ScrollOffset.Y);
        if (visualLine?.FirstDocumentLine is null)
        {
            return;
        }

        int line = visualLine.FirstDocumentLine.LineNumber;
        MarginClicked?.Invoke(this, line);
        e.Handled = true;
    }

    private bool TryGetLineCenterY(int lineNumber, out double y)
    {
        y = 0;
        TextView? textView = TextView;
        if (textView is null || !textView.VisualLinesValid)
        {
            return false;
        }

        foreach (VisualLine visualLine in textView.VisualLines)
        {
            if (visualLine.FirstDocumentLine.LineNumber != lineNumber)
            {
                continue;
            }

            y = visualLine.GetTextLineVisualYPosition(visualLine.TextLines[0], VisualYPosition.TextMiddle) - textView.ScrollOffset.Y;
            return true;
        }

        return false;
    }
}
