from typing import Iterator

from SQLDbReader import SQLDbReader
from WDBDataclasses.WDBBord import WDBBord
from WDBDataclasses.WDBOpstelling import WDBOpstelling


class SQLiteQueryExecutor:
    def __init__(self, sql_db_reader: SQLDbReader):
        self.sql_db_reader = sql_db_reader

    def get_all_opstellingen(self) -> Iterator[WDBOpstelling]:
        data = self.sql_db_reader.perform_read_query(
            "SELECT id, zijdeVanDeRijweg, status, wegsegmentid FROM opstelling ORDER BY id",
            {})

        for row in data:
            yield WDBOpstelling(id=row[0], zijde_van_de_rijweg=row[1], status=row[2], wegsegment_id=row[3])

    def get_all_borden(self, opstelling_ids: [int]) -> Iterator[WDBBord]:
        idstring = '(' + ','.join(map(str, opstelling_ids)) + ')'
        query = "SELECT borden.id, aanzichten.hoek, aanzichten.opstelling_fk, y, borden.parameters, borden.code " \
                "FROM aanzichten " \
                "LEFT JOIN borden on borden.aanzicht_fk = aanzichten.id " \
                f"WHERE aanzichten.opstelling_fk in {idstring} " \
                "ORDER BY aanzichten.opstelling_fk, aanzichten.id , borden.id"
        data = self.sql_db_reader.perform_read_query(query, {})

        for row in data:
            yield WDBBord(id=row[0], hoek=row[1], opstelling_id=row[2], y=row[3], parameters=row[4], code=row[5])
