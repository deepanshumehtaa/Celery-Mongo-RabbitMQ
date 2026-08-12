import logging
from typing import Any, MutableMapping

class TraceLoggerAdapter(logging.LoggerAdapter):
    """
    Custom LoggerAdapter wrapper that formats log messages to include [trace_id: <id>].
    """
    def __init__(self, logger: logging.Logger, trace_id: str = "N/A"):
        super().__init__(logger, {"trace_id": trace_id or "N/A"})
        self.trace_id = trace_id or "N/A"

    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        # Check if a custom trace_id was passed directly in log call, otherwise use default
        t_id = kwargs.pop("trace_id", self.trace_id)
        formatted_msg = f"[trace_id: {t_id}] {msg}"
        return formatted_msg, kwargs

    def set_trace_id(self, trace_id: str):
        """Updates the trace_id associated with this logger wrapper instance."""
        self.trace_id = trace_id or "N/A"
        self.extra["trace_id"] = self.trace_id

def get_trace_logger(name: str, trace_id: str = None) -> TraceLoggerAdapter:
    """
    Factory function returning a TraceLoggerAdapter wrapping the standard Python logger.
    """
    raw_logger = logging.getLogger(name)
    return TraceLoggerAdapter(raw_logger, trace_id=trace_id)
