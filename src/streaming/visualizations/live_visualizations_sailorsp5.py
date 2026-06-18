"""src/streaming/visualizations/live_visualizations_sailorsp5.py.

Project-specific live visualization functions used by the Kafka consumer.

This module creates a live line chart of sale total by message.
The chart opens in a window while the consumer is running and updates
as each message is consumed.

Author: Sydney Sailors
Date: 2026-06

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it live_visualizations_yourname.py,
  and modify your copy for your own charts.
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

# === DECLARE EXPORTS ===

# Use the built-in __all__ variable to declare a list of
# public objects that this module exports.
# This is a common Python convention that helps other developers understand
# which functions are intended for use outside this module.

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


# === DEFINE LIVE CHART HELPERS ===


def init_live_chart() -> tuple[Any, Any, list[int], list[float]]:
    """Initialize and display a live Matplotlib chart for streaming sales data.

    Returns:
        A tuple containing:
        - figure: Matplotlib figure object
        - axis: Matplotlib axis object
        - x_values: list of message offsets
        - y_values: list of sales totals
    """
    plt.ion()

    fig, ax = plt.subplots()

    x_values: list[int] = []
    y_values: list[float] = []

    ax.set_title("Cumulative Sales Over Time")
    ax.set_xlabel("Message")
    ax.set_ylabel("Cumulative Sales ($)")

    fig.show()
    fig.canvas.draw()
    fig.canvas.flush_events()

    return fig, ax, x_values, y_values


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
    message: dict[str, Any],
) -> None:
    """Update the live sales chart with a new Kafka message.

    Extracts the message offset as the x-axis value and the
    sales total as the y-axis value, then redraws the chart.

    Args:
        figure: Matplotlib figure object.
        axis: Matplotlib axis object.
        x_values: Existing x-axis values (message offsets).
        y_values: Existing y-axis values (sales totals).
        message: Enriched Kafka message containing a "total" field.
    """
    new_x = len(x_values) + 1
    x_values.append(new_x)

    last_total = y_values[-1] if y_values else 0
    new_total = last_total + float(message["total"])
    y_values.append(new_total)

    axis.clear()
    axis.plot(x_values, y_values, marker="o")

    axis.set_title("Cumulative Sales Over Time")
    axis.set_xlabel("Message")
    axis.set_ylabel("Cumulative Sales ($)")
    axis.grid(True)

    figure.canvas.draw()
    figure.canvas.flush_events()
    plt.pause(0.05)


def save_live_chart(
    *,
    figure: Any,
    chart_path: Path,
) -> None:
    """Save the final live chart to an image file.

    All arguments after the asterisk (*) must be passed as keyword arguments.

    Arguments:
        figure: Matplotlib figure.
        chart_path: Output image path.

    Returns:
        None.
    """
    # Ensure the output directory exists before saving the figure.
    chart_path.parent.mkdir(parents=True, exist_ok=True)

    # Use the figure.savefig() method to save the chart to an image file.
    # Use the bbox_inches="tight" argument to ensure the saved image is cropped to the content of the chart.
    figure.savefig(chart_path, bbox_inches="tight")


def close_live_chart() -> None:
    """Turn off interactive chart mode."""
    # Call plt.ioff() to turn off interactive mode when the consumer is finished.
    plt.ioff()
