import abc
from collections.abc import Generator

import pandas

from app.gen.client.adminapi import models


class UploaderPlugin(abc.ABC):
    @abc.abstractmethod
    def prepare(self) -> None:
        """
        Makes any necessary preparations before starting the uploading process.
        For example: opens database connection, gets authentication tokens, opens file descriptors, etc.
        """

    @abc.abstractmethod
    def get_schema(self) -> list[models.ColumnDescription]:
        """
        Obtains the list of columns that describe the data.
        These columns might have any metadata required.
        This metadata will be used later to mark the columns and distinguish between different units.
        """

    @abc.abstractmethod
    def get_data(self) -> Generator[tuple[pandas.DataFrame, float]]:
        """
        Yields DataFrames that represent the data from the table.
        Not all of the columns from the `get_schema` method must be present but there should be no columns
        that were not returned from `get_schema`.
        This method will yield tuples of (DataFrame, completion_rate) until all data is processed.

        The float returned is the completion rate in the range [0, 1]. It will be displayed to the user.
        """

    @abc.abstractmethod
    def stop(self) -> None:
        """
        Closes any necessary connections and cleans up any residuals after the uploading process.
        This method will be called after the completion of the uploading or during the graceful shutdown
        in case any unrecoverable errors occur.
        """


class DefaultTableNamer(abc.ABC):
    @abc.abstractmethod
    def get_table_name(self) -> str:
        pass


class BibcodeProvider(abc.ABC):
    @abc.abstractmethod
    def get_bibcode(self) -> str:
        pass


class DescriptionProvider(abc.ABC):
    @abc.abstractmethod
    def get_description(self) -> str:
        pass
