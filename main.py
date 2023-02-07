
from pathlib import Path

from Processor import Processor
from SQLDbReader import SQLDbReader
from SQLiteQueryExecutor import SQLiteQueryExecutor


if __name__ == '__main__':
    processor = Processor(SQLiteQueryExecutor(SQLDbReader(Path('verkeersborden20k.sqlite'))),
                          bord_register=Path('wegcode_register.csv'))
    processor.process()


# html table scraping:
# https://www.convertcsv.com/html-table-to-csv.htm
# https://www.wegcode.be/nl/regelgeving/1975120109~hra8v386pu#sb9oiiegjk