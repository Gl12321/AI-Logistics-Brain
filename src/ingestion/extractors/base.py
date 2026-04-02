from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any

class BaseExtractor(ABC):

    @abstractmethod
    def parse(self, file_path: Path) -> List[Dict[str, Any]]:
        pass