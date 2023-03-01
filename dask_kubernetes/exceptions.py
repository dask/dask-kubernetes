class CrashLoopBackOffError(Exception):
    """Cluster got stuck in CrashLoopBackOff."""


class SchedulerStartupError(Exception):
    """Scheduler failed to start."""
