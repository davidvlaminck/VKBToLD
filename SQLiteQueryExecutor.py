from typing import Iterator

from SQLDbReader import SQLDbReader
from WDBDataclasses.WDBBeugel import WDBBeugel
from WDBDataclasses.WDBBord import WDBBord
from WDBDataclasses.WDBOphanging import WDBOphanging
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
        query = "SELECT borden.id, aanzichten.hoek, aanzichten.opstelling_fk, y, borden.parameters, borden.code, " \
                "   borden.folieType, borden.vorm, borden.breedte, borden.hoogte " \
                "FROM aanzichten " \
                "   LEFT JOIN borden on borden.aanzicht_fk = aanzichten.id " \
                f"WHERE aanzichten.opstelling_fk in {idstring} " \
                "ORDER BY aanzichten.opstelling_fk, aanzichten.id , borden.id"
        data = self.sql_db_reader.perform_read_query(query, {})

        for row in data:
            yield WDBBord(id=row[0], hoek=row[1], opstelling_id=row[2], y=row[3], parameters=row[4], code=row[5],
                          folie_type=row[6], vorm=row[7], breedte=row[8], hoogte=row[9])

    def get_all_ophangingen(self, opstelling_ids: [int]) -> Iterator[WDBOphanging]:
        idstring = '(' + ','.join(map(str, opstelling_ids)) + ')'
        query = "SELECT ophangingen.id, clientId, ophangingen.lengte, diameter, opstelling_fk, sokkelAfmetingen.naam " \
                "FROM ophangingen " \
                "LEFT JOIN sokkelAfmetingen on sokkelAfmetingen.key = sokkelAfmetingen_fk " \
                f"WHERE ophangingen.opstelling_fk in {idstring} " \
                "ORDER BY ophangingen.opstelling_fk, id"
        data = self.sql_db_reader.perform_read_query(query, {})

        for row in data:
            yield WDBOphanging(id=row[0], client_id=row[1], lengte=row[2], diameter=row[3], opstelling_id=row[4],
                               sokkel_naam=row[5])

    def get_all_beugels(self, ophanging_ids: [int]) -> Iterator[WDBBeugel]:
        idstring = '(' + ','.join(map(str, ophanging_ids)) + ')'
        query = "SELECT bevestigingen.id, ophanging_fk, bord_fk " \
                "FROM bevestigingen " \
                "LEFT JOIN bevestigingsprofielen bp ON bp.id = bevestigingen.bevestigingsprofiel_fk " \
                f"WHERE ophanging_fk in {idstring} " \
                "ORDER BY ophanging_fk, bevestigingen.id" \

        data = self.sql_db_reader.perform_read_query(query, {})

        for row in data:
            yield WDBBeugel(id=row[0], ophanging_id=row[1], bord_id=row[2])
