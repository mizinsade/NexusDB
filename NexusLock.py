import fcntl
from contextlib import contextmanager

class NexusLock:
    def __init__(self, file_handle):
        self.f = file_handle

    @contextmanager
    def shared(self):
        """읽기 잠금: 여러 프로세스가 동시에 읽을 수 있음"""
        try:
            fcntl.flock(self.f, fcntl.LOCK_SH)
            yield
        finally:
            fcntl.flock(self.f, fcntl.LOCK_UN)

    @contextmanager
    def exclusive(self):
        """쓰기 잠금: 단 한 명의 프로세스만 접근 가능"""
        try:
            fcntl.flock(self.f, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(self.f, fcntl.LOCK_UN)