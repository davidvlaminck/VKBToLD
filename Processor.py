import math

from rdflib import Graph, URIRef, RDF, Namespace, Literal, BNode, XSD

from SQLiteQueryExecutor import SQLiteQueryExecutor
from WDBDataclasses.WDBOpstelling import WDBOpstelling


class Processor:
    def __init__(self, executor: SQLiteQueryExecutor = None):
        self.executor = executor
        self.graph_counter = 0

    def process(self, batch_size: int = 100):
        g = self.write_and_create_graph(None)

        opstelling_ids = []
        for opstelling in self.executor.get_all_opstellingen():
            opstelling_ids.append(opstelling.id)
            self.process_opstelling(g, opstelling)

            if len(opstelling_ids) >= batch_size:
                self.process_borden(g, opstelling_ids)
                g = self.write_and_create_graph(g)
                opstelling_ids = []

        self.write_and_create_graph(g)

    def write_and_create_graph(self, g) -> Graph:
        if g is not None:
            print(f'wrote test_{self.graph_counter}.ttl with {len(g)} triples')
            g.serialize(destination=f'test_{self.graph_counter}.ttl')

        self.graph_counter += 1
        g = Graph()
        g.bind('asset', Namespace('https://data.awvvlaanderen.be/id/asset/'))
        g.bind('installatie', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#'))
        g.bind('wr', Namespace('https://www.vlaanderen.be/digitaal-vlaanderen/onze-oplossingen/wegenregister/'))
        g.bind('imel', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#'))
        g.bind('abs', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#'))
        g.bind('sign', Namespace('https://wegenenverkeer.data.vlaanderen.be/doc/implementatiemodel/signalisatie/#'))
        g.bind('concept', Namespace('https://wegenenverkeer.data.vlaanderen.be/id/concept/'))
        g.bind('onderdeel', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#'))
        return g

    def process_opstelling(self, g, opstelling):
        self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/opstelling_{opstelling.id}')
        g.add((self_uri, RDF.type, URIRef(
            'https://wegenenverkeer.data.vlaanderen.be/doc/implementatiemodel/signalisatie/#Verkeersbordopstelling')))
        g.add((self_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Verkeersbordopstelling.wegSegment'),
               URIRef(
                   f'https://www.vlaanderen.be/digitaal-vlaanderen/onze-oplossingen/wegenregister/{opstelling.wegsegment_id}')))
        self.add_positie_rijweg_to_opstelling(g, self_uri, opstelling)
        asset_id_node = BNode()
        g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.assetId'),
               asset_id_node))
        g.add((asset_id_node, URIRef(
            'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator'),
               Literal(f'opstelling_{opstelling.id}')))

    def add_positie_rijweg_to_opstelling(self, g: Graph, self_uri: URIRef, opstelling: WDBOpstelling):
        if opstelling.zijde_van_de_rijweg == "LINKS":
            g.add((self_uri,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Verkeersbordopstelling.positieTovRijweg'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlPositieSoort/linkerrand')))
        elif opstelling.zijde_van_de_rijweg == "RECHTS":
            g.add((self_uri,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Verkeersbordopstelling.positieTovRijweg'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlPositieSoort/rechterrand')))
        elif opstelling.zijde_van_de_rijweg == "MIDDEN":
            g.add((self_uri,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Verkeersbordopstelling.positieTovRijweg'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlPositieSoort/midden')))
        else:
            raise NotImplementedError(f"Verkeersbordopstelling.positieTovRijweg can't be mapped to it: "
                                      f"{opstelling.zijde_van_de_rijweg}")

    def process_borden(self, g: Graph, opstelling_ids: [int]):
        bord_ids = []
        for bord in self.executor.get_all_borden(opstelling_ids):
            bord_ids.append(bord.id)
            self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/bord_{bord.id}')
            opstelling_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/opstelling_{bord.opstelling_id}')

            # TODO onderbord
            g.add((self_uri, RDF.type,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendVerkeersbord')))

            # hoortbij relatie
            relatie_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/bord_{bord.id}-opstelling_{bord.opstelling_id}')
            g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
                   self_uri))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
                   opstelling_uri))

            # aanzicht
            aanzicht_hoek = round(bord.hoek * 180.0 / math.pi, 1)
            while aanzicht_hoek < 0:
                aanzicht_hoek += 360.0
            if aanzicht_hoek > 360.0:
                aanzicht_hoek = aanzicht_hoek % 360.0
            aanzicht_kwant_node = BNode()
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Verkeersbord.aanzicht'),
                   aanzicht_kwant_node))
            g.add((aanzicht_kwant_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInDecimaleGraden.waarde'),
                   Literal(aanzicht_hoek, datatype=XSD.decimal)))

            # opstelhoogte
            if bord.y is not None and bord.y > 0:
                hoogte_kwant_node = BNode()
                g.add((self_uri,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Verkeersbord.opstelhoogte'),
                       hoogte_kwant_node))
                g.add((hoogte_kwant_node,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMeter.waarde'),
                       Literal(bord.y / 1000.0, datatype=XSD.decimal)))
