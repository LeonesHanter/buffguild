class ObserverBot:
    def __init__(self, tm, executor):
        self.tm = tm
        self.executor = executor

        self.scheduler = Scheduler(tm, executor, on_buff_complete=self._handle_buff_completion)
        self.health_monitor = TokenHealthMonitor(tm)

        self.observer = tm.get_observer()
        ...
        self._active_jobs: Dict[int, ActiveJobInfo] = {}
        self._buff_results: Dict[int, BuffResultInfo] = {}

        # НОВОЕ: persistent storage активных бафов
        self._job_storage = JobStorage(path="jobs.json")
        self._restore_active_jobs()

        ...
