from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from genericpath import isfile
from io import BufferedReader
from typing import IO, Any, List, Literal, Optional, Union

from blobular.cli.console import user_info
from ..store import AbstractBlobStore
from .settings import Settings, logger
from .state import AppState
import os.path
from blake3 import blake3
import os
import shutil
from pathlib import Path, PurePath
import pathlib
from stat import S_IREAD, S_IRGRP, S_IROTH

BLOCK_SIZE = 2**20


def get_filestore():
    return AppState.current().local_file_store


@dataclass
class FileSnapshot:
    """This represents the state of a file on the host machine at a particular point in time.
    Saved methods can return these to make it clear that a function's result is stored in a file.
    You can then read this file directly.
    """

    relpath: Optional[Path]
    """ original path on host machine, relative to the workspace directory.

    If it is None, then the file was snapshotted outside of the workspace directory."""

    name: str
    """ The name of the file. """

    digest: str
    """ BLAKE3 hash of the file """

    time: datetime
    """ Time at which the file was snap-shotted. """

    content_length: int
    """ Number of bytes of the file. """

    @property
    def has_local_cache(self):
        """Returns true if the file has already been cached."""
        return get_filestore().has(self.digest)

    @property
    def local_cache_path(self):
        return AppState.current().local_file_store.local_file_cache_path(self.digest)

    @property
    def suffix(self):
        """The suffix extension of the file. Eg ``hello.txt`` has the suffix ``.txt``."""
        return Path(self.name).suffix

    def download(self):
        """Download the file from the cloud to the local file cache."""
        a = AppState.current()
        # [todo] tidy this up, make it more clear.
        local = a.store.cache
        files = a.local_file_store
        a.store.pull(self.digest)
        if files.has(self.digest):
            return
        assert local.has(self.digest)  # because we pulled it
        with local.open(self.digest) as f:
            # [todo] remove tempfiles implicitly used here.
            # [todo] need to tell SizedBlobStore that the file is stored on disk.
            files.add(f, digest=self.digest, content_length=self.content_length)
        user_info(f"Downloaded {self.name}.")

    def open(self) -> IO[bytes]:
        """Open the snapshot in read mode. (writing to a snapshot is not allowed.)"""
        return get_filestore().open(digest=self.digest)

    def restore_at(self, path: Path, overwrite: Optional[bool] = None) -> Path:
        """Restore the file at the given path. Returns the path of the restored file.
        If the given path is a directory, the file will be stored in the directory with the file's given basename.
        Otherwise the file will be saved at the exact path.
        If path has a different extension to the snapshot's path extension then we emit a value-error, because it's likely there was a mistake.
        """
        # [todo] directory exists
        # [todo] warn if overwriting
        # [todo] also we need to be aware of security auditing: https://peps.python.org/pep-0578/
        if not path.parent.exists():
            raise ValueError(f"Path {path.parent} does not exist.")
        if path.is_dir():
            user_info(
                f"restore_at: {path} is a directory so appending the basename of the file {self.name}."
            )
            path = path / self.name
        if self.suffix != path.suffix:
            raise ValueError(
                f"Refusing to write to {path} since the extension name is different to extension of {self.relpath}"
            )
        if path.is_symlink():
            # [todo] if it links to our local cache then this is fine.
            # double check that the file exists
            logger.warn(f"restore over a symlink not implemented. {path}")
            pass
        if path.exists():
            if overwrite is False:
                raise FileExistsError(
                    f"Refusing to restore: would overwrite {path} and overwrite is set to False."
                )
            else:
                # [todo] this can cause damage, we should make a new snapshot of this file so that we don't lose data.
                if overwrite is None:
                    logger.warn(
                        f"File {path} already exists, replacing with a symlink to {self.local_cache_path}.",
                        f"To suppress this warning, explicitly pass overwrite=True to restore().",
                    )
                elif overwrite is not True:
                    raise TypeError("overwrite must be True, False or None")
                path.unlink()

        self.download()
        path.symlink_to(self.local_cache_path)
        return path

    def restore(self, overwrite: Optional[bool] = None, project_path=None) -> Path:
        """Write the snapshot back to its original location (given by relpath).
        Returns the absolute path of the file that was restored."""
        if not self.has_local_cache:
            self.download()
        if self.relpath is None:
            raise ValueError(f"Can't restore a snapshot without a specific path.")
        project_path = project_path or Settings.current().workspace_dir
        if project_path is None:
            raise ValueError(
                f"Can't restore a snapshot without a workspace directory to place relative to."
            )
        project_path = Path(project_path)
        abspath = project_path / self.relpath
        assert abspath.is_relative_to(project_path)
        return self.restore_at(abspath, overwrite=overwrite)

    def restore_safe(self) -> Path:
        """Downloads the file snapshot and returns a path to the snapshot. Guaranteed not to overwrite anything. rename to get_path"""
        if not self.has_local_cache:
            self.download()
        return self.local_cache_path

    @classmethod
    def snap(cls, path: Union[Path, str], workspace_dir=None):
        """Make a snapshot of the file at the given path.

        The path is stored relative to your workspace directory (either the location of your `pyproject.toml` or git root).
        """
        # [todo] accept file-like objects as well as paths.
        path = Path(path).resolve()
        # [todo] assert path is within project.
        # We should be very very careful about saving files from some arbitrary part of the disk.
        # [todo] assert that the file is finite (eg not `/dev/yes`)
        # [todo] do we need to lock files in case of multiprocessing? This is faff crossplatform
        workspace_dir = workspace_dir or Settings.current().workspace_dir

        if workspace_dir is not None and path.is_relative_to(workspace_dir):
            relpath = path.relative_to(workspace_dir)
        else:
            relpath = None
        time = datetime.now()
        with open(path, "rb") as fd:
            r = get_filestore().add(fd)
        snap = FileSnapshot(
            digest=r.digest,
            time=time,
            relpath=relpath,
            content_length=r.content_length,
            name=path.name,
        )
        return snap

    @property
    def is_uploaded(self):
        return AppState.current().cloud_store.has(self.digest)

    def upload(self):
        if not AppState.current().store.has(self.digest):
            raise RuntimeError(f"file {self.digest} was not added")
        try:
            if AppState.current().store.push(self.digest):
                user_info(f"Uploaded {self.name}.")
        except ConnectionError as e:
            logger.error(f"no connection: {e}")


@dataclass
class DirectorySnapshot:
    """Similar to FileSnapshot, but snaps an entire directory."""

    # [todo]: archiving mode where it saves a directory as a .zip, .tar.gz or similar.

    original_path: Path
    """ Path on users machine when the snapshot was taken. """

    relpath: Optional[Path]
    """ Path relative to the workspace directory of the code that snapshotted this directory.

    If it is none, then the snapshot was not taken on a directory that is in the workspace.
    """
    files: List[FileSnapshot]
    digest: str
    """ All files (including files in subdirectories),  """

    def download(self):
        for f in self.files:
            f.download()

    def restore_at(self, path: Path, overwrite=True) -> Path:
        """Restores the directory at the given path. Returns the path to the root of the snapshotted directory. (which is the same as the path argument)."""
        if path.exists():
            if overwrite:
                user_info(
                    f"{path} already exists, files that are also present in the directory snapshot will be overwritten, other files will be left alone."
                )
        for file in self.files:
            assert (
                file.relpath is not None
            ), "malformed file snapshot in directory snapshot"
            filepath = path / file.relpath
            assert filepath.is_relative_to(
                path
            ), f"modification of files outside {path} is not allowed"

            filepath.parent.mkdir(parents=True, exist_ok=True)
            try:
                file.restore(overwrite=overwrite, project_path=path)
            except FileExistsError as e:
                user_info(f"File {filepath} already exists, skipping.")
        return path

    def restore_safe(self) -> Path:
        """Restores the directory to a location in hitsave's cache directory (where we know that there is no chance of overwriting existing files)."""
        path: Path = (
            Settings.current().local_cache_dir / "directory_snaps" / self.digest[:10]
        )
        if path.exists():
            user_info(f"Directory snapshot already present {path}.")
            # [todo] check that the blobs have not been removed.
            return path
        path.mkdir(exist_ok=True, parents=True)
        return self.restore_at(path)

    def restore(self, workspace_dir=None, overwrite=True) -> Path:
        """Restores the snapshotted directory. Returns the path of the directory that was restored."""
        workspace_path = Path(workspace_dir or Settings.current().workspace_dir)
        if self.relpath is None:
            raise ValueError(
                "Can't restore directory without specific path. Try using restore_safe."
            )
        abspath = workspace_path / self.relpath
        user_info(f"Restoring directory snapshot at {abspath}.")
        self.restore_at(abspath, overwrite=overwrite)
        return abspath

    @classmethod
    def snap(cls, path, workspace_dir=None):
        path = Path(path).resolve()
        workspace_dir = workspace_dir or Settings.current().workspace_dir
        if workspace_dir is not None and path.is_relative_to(workspace_dir):
            relpath = path.relative_to(workspace_dir)
        else:
            relpath = None

        def rec(p: Path):
            for child_path in p.iterdir():
                if child_path.is_symlink():
                    # [todo] should be a warning
                    raise NotImplementedError(
                        "Directory snapshots containing symlinks is not supported yet."
                    )
                if child_path.is_dir():
                    yield from rec(child_path)
                if child_path.is_file():
                    yield FileSnapshot.snap(child_path, workspace_dir=path)

        files = sorted(rec(path), key=lambda x: x.relpath or 0)
        user_info(f"Directory snapshot created for {len(files)} files.")
        h = blake3()
        for file in files:
            h.update(file.digest.encode())
        digest = h.hexdigest()
        snap = cls(relpath=relpath, files=files, digest=digest, original_path=path)
        return snap

    def upload(self):
        # [todo] add progress bar here.
        for file in self.files:
            file.upload()
        # [todo] need an extra step here where we upload a blob containing the directory info.
