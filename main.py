
from pathlib import Path

from Processor import Processor
from SQLDbReader import SQLDbReader
from SQLiteQueryExecutor import SQLiteQueryExecutor


if __name__ == '__main__':
    processor = Processor(SQLiteQueryExecutor(SQLDbReader(Path('verkeersborden300.sqlite'))))
    processor.process()
