from abc import ABC, abstractmethod


class DataHandler(ABC):
    """Abstract Base Class for fetching market data."""
    @abstractmethod
    def fetch_next_bar(self):
        """Returns the next available OHLCV bar."""
        pass
