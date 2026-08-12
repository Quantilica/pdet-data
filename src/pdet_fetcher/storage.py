from pathlib import Path

from quantilica.core.dates import year_month_partition
from quantilica.core.storage import (
    BaseDataRepository,
    build_stamped_filename,
    stamp_filename,
)


class DataRepository(BaseDataRepository):
    """Manages local storage for PDET files using BaseDataRepository."""

    def __init__(self, root: Path | str):
        """Initializes the DataRepository.

        Args:
            root (Path | str): The root directory for the repository.
        """
        super().__init__(root)

    def get_docs_filepath(self, file_metadata: dict) -> Path:
        """Gets the local file path for documentation files.

        Args:
            file_metadata (dict): The metadata of the file.

        Returns:
            Path: The resolved local path.
        """
        dataset = file_metadata["dataset"]
        filename = get_docs_filename(file_metadata)
        return self.storage.path_for(f"_documentacao/{dataset}/{filename}")

    def get_caged_filepath(self, file_metadata: dict) -> Path:
        """Gets the local file path for classic CAGED files.

        Args:
            file_metadata (dict): The metadata of the file.

        Returns:
            Path: The resolved local path.
        """
        dataset = file_metadata["dataset"]
        year = str(file_metadata["year"])
        filename = get_caged_filename(file_metadata)
        return self.dataset_path(dataset, year, filename)

    def get_caged_2020_filepath(self, file_metadata: dict) -> Path:
        """Gets the local file path for Novo CAGED files.

        Args:
            file_metadata (dict): The metadata of the file.

        Returns:
            Path: The resolved local path.
        """
        dataset = file_metadata["dataset"]
        year = str(file_metadata["year"])
        filename = get_caged_2020_filename(file_metadata)
        return self.dataset_path(dataset, year, filename)

    def get_rais_filepath(self, file_metadata: dict) -> Path:
        """Gets the local file path for RAIS files.

        Args:
            file_metadata (dict): The metadata of the file.

        Returns:
            Path: The resolved local path.
        """
        dataset = file_metadata["dataset"]
        year = str(file_metadata["year"])
        filename = get_rais_filename(file_metadata)
        return self.dataset_path(dataset, year, filename)


def get_docs_filename(file_metadata: dict) -> str:
    """Generates a stamped filename for documentation files.

    Args:
        file_metadata (dict): The metadata of the file.

    Returns:
        str: The generated filename.
    """
    name, _ = file_metadata["name"].rsplit(".", maxsplit=1)
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return stamp_filename(name, extension, modified)


def get_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for documentation files using a destination directory.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return DataRepository(dest_dir).get_docs_filepath(file_metadata)


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def get_caged_filename(file_metadata: dict) -> str:
    """Generates a stamped filename for classic CAGED files.

    Args:
        file_metadata (dict): The metadata of the file.

    Returns:
        str: The generated filename.
    """
    dataset = file_metadata["dataset"]
    year = file_metadata["year"]
    partition = year_month_partition(year, file_metadata.get("month"))
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return build_stamped_filename(dataset, partition, ext=extension, timestamp=modified)


def get_caged_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for classic CAGED files.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return DataRepository(dest_dir).get_caged_filepath(file_metadata)


def get_caged_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for classic CAGED documentation files.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return get_docs_filepath(file_metadata, dest_dir)


def get_caged_2020_filename(file_metadata: dict) -> str:
    """Generates a stamped filename for Novo CAGED files.

    Args:
        file_metadata (dict): The metadata of the file.

    Returns:
        str: The generated filename.
    """
    dataset = file_metadata["dataset"]
    year = file_metadata["year"]
    month = file_metadata["month"]
    partition = year_month_partition(year, month)
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return build_stamped_filename(dataset, partition, ext=extension, timestamp=modified)


def get_caged_2020_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for Novo CAGED files.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return DataRepository(dest_dir).get_caged_2020_filepath(file_metadata)


def get_caged_2020_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for Novo CAGED documentation files.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return get_docs_filepath(file_metadata, dest_dir)


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def get_rais_filename(file_metadata: dict) -> str:
    """Generates a stamped filename for RAIS files.

    Args:
        file_metadata (dict): The metadata of the file.

    Returns:
        str: The generated filename.
    """
    dataset = file_metadata["dataset"]
    year = file_metadata["year"]
    partition = f"{year}"
    if region := file_metadata.get("uf", file_metadata.get("region")):
        partition = partition + f"-{region}"
    modified = file_metadata["datetime"]
    extension = file_metadata["extension"]
    return build_stamped_filename(dataset, partition, ext=extension, timestamp=modified)


def get_rais_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for RAIS files.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return DataRepository(dest_dir).get_rais_filepath(file_metadata)


def get_rais_docs_filepath(file_metadata: dict, dest_dir: Path) -> Path:
    """Gets the local file path for RAIS documentation files.

    Args:
        file_metadata (dict): The metadata of the file.
        dest_dir (Path): The base destination directory.

    Returns:
        Path: The resolved local path.
    """
    return get_docs_filepath(file_metadata, dest_dir)
