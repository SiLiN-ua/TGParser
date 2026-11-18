import os
import sys
from pathlib import Path


class ProcessLock:
    def __init__(self, path: str):
        self.path = Path(path)
        self._fh = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if sys.platform == "win32":
            self._acquire_win()
        else:
            self._acquire_unix()
        self._fh.write(str(os.getpid()))
        self._fh.flush()

    def _acquire_win(self):
        import msvcrt
        self._fh = open(self.path, "a+")
        self._fh.seek(0)
        try:
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as e:
            self._fh.close()
            self._fh = None
            raise RuntimeError(f"Another TGParser is running (lock {self.path})") from e
        self._fh.seek(0)
        self._fh.truncate()

    def _acquire_unix(self):
        import fcntl
        self._fh = open(self.path, "w")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            self._fh.close()
            self._fh = None
            raise RuntimeError(f"Another TGParser is running (lock {self.path})") from e

    def release(self):
        if self._fh is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt
                try:
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                except Exception:
                    pass
            self._fh.close()
        finally:
            self._fh = None
            try:
                self.path.unlink()
            except Exception:
                pass
