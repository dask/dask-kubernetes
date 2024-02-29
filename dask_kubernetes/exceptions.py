class CrashLoopBackOffError(Exception):
    """Cluster got stuck in CrashLoopBackOff."""


class SchedulerStartupError(Exception):
    """Scheduler failed to start."""


class ValidationError(Exception):
    """Manifest validation exception"""

    def __init__(self, message: str) -> None:
        self.message = message
