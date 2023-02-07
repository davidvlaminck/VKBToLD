import csv
import math
from pathlib import Path

from rdflib import Graph, URIRef, RDF, Namespace, Literal, BNode, XSD

from SQLiteQueryExecutor import SQLiteQueryExecutor
from WDBDataclasses.WDBBord import WDBBord
from WDBDataclasses.WDBOpstelling import WDBOpstelling


class Processor:
    def __init__(self, executor: SQLiteQueryExecutor = None, bord_register: Path = None):
        self.executor = executor
        self.graph_counter = 0
        self.bord_register = {}
        if bord_register is not None:
            self.add_borden_register(bord_register)
        self.bord_register_not_found = set()

    def process(self, batch_size: int = 100, write_size: int = 20000):
        g = self.write_and_create_graph(None)

        opstelling_ids = []
        for write_count, opstelling in enumerate(self.executor.get_all_opstellingen()):
            opstelling_ids.append(opstelling.id)
            self.process_opstelling(g, opstelling)

            if len(opstelling_ids) >= batch_size:
                self.process_borden(g, opstelling_ids)
                opstelling_ids = []

            if write_count > 0 and write_count % write_size == 0:
                g = self.write_and_create_graph(g)

        self.write_and_create_graph(g)
        print('could not find info in register for following signs:')
        print(', '.join(sorted(self.bord_register_not_found)))

    def write_and_create_graph(self, g) -> Graph:

        if g is not None:
            amount_triples = len(g)
            if amount_triples > 0:
                g.serialize(destination=f'test_{self.graph_counter}.ttl')
                print(f'wrote test_{self.graph_counter}.ttl with {amount_triples} triples')

        self.graph_counter += 1
        g = Graph()
        g.bind('asset', Namespace('https://data.awvvlaanderen.be/id/asset/'))
        g.bind('installatie', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#'))
        g.bind('wr', Namespace('https://www.vlaanderen.be/digitaal-vlaanderen/onze-oplossingen/wegenregister/'))
        g.bind('imel', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#'))
        g.bind('abs', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#'))
        g.bind('sign', Namespace('https://wegenenverkeer.data.vlaanderen.be/doc/implementatiemodel/signalisatie/#'))
        g.bind('kl', Namespace('https://wegenenverkeer.data.vlaanderen.be/id/concept/'))
        g.bind('onderdeel', Namespace('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#'))
        g.bind('wegcode', Namespace('https://www.wegcode.be/media/image/orig/'))
        return g

    def process_opstelling(self, g, opstelling):
        self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/opstelling_{opstelling.id}')
        g.add((self_uri, RDF.type, URIRef(
            'https://wegenenverkeer.data.vlaanderen.be/doc/implementatiemodel/signalisatie/#Verkeersbordopstelling')))

        wegsegment_node = BNode()
        g.add((self_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Verkeersbordopstelling.wegSegment'),
               wegsegment_node))
        g.add((wegsegment_node,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtcExterneReferentie.externReferentienummer'),
               Literal(str(opstelling.wegsegment_id))))
        g.add((wegsegment_node,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtcExterneReferentie.externePartij'),
               Literal('WegenRegister')))

        self.add_positie_rijweg_to_opstelling(g, self_uri, opstelling)
        asset_id_node = BNode()
        g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#AIMObject.assetId'),
               asset_id_node))
        g.add((asset_id_node, URIRef(
            'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcIdentificator.identificator'),
               Literal(f'opstelling_{opstelling.id}')))

    def add_positie_rijweg_to_opstelling(self, g: Graph, self_uri: URIRef, opstelling: WDBOpstelling):
        if opstelling.zijde_van_de_rijweg is None:
            return

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
            if opstelling.zijde_van_de_rijweg == "BOVEN":
                return
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
            # TODO calamiteisbord
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

            self.process_teken(g=g, bord=bord, bord_uri=self_uri)

    def process_teken(self, g: Graph, bord: WDBBord, bord_uri: URIRef):
        self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/verkeersteken_{bord.id}')

        g.add((self_uri, RDF.type,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerkeersbordVerkeersteken')))

        # hoortbij relatie
        relatie_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/bord_{bord.id}-verkeersteken_{bord.id}')
        g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')))
        g.add((relatie_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
               bord_uri))
        g.add((relatie_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
               self_uri))

        # variabelOpschrift
        if bord.parameters is not None:
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#Verkeersteken.variabelOpschrift'),
                   Literal(bord.parameters)))

        self.process_concept(g, bord=bord, concept_uri=self_uri)

    def process_concept(self, g: Graph, bord: WDBBord, concept_uri: URIRef):
        self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/verkeersbordconcept_{bord.id}')

        g.add((self_uri, RDF.type,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerkeersbordConcept')))

        # hoortbij relatie
        relatie_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/verkeersteken_{bord.id}-verkeersbordconcept_{bord.id}')
        g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')))
        g.add((relatie_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
               concept_uri))
        g.add((relatie_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
               self_uri))

        # code
        if bord.code != 'Unknown' and bord.code is not None:
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerkeersbordConcept.verkeersbordCode'),
                   Literal(bord.code)))

            if bord.code in self.bord_register:
                g.add((self_uri,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerkeersbordConcept.betekenis'),
                       Literal(self.bord_register[bord.code]['betekenis'])))
                image_uri = URIRef(f"https://www.wegcode.be{self.bord_register[bord.code]['image']}")
                image_filename = self.bord_register[bord.code]['image'].split('/')[-1]
                afbeelding_node = BNode()
                g.add((self_uri,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerkeersbordConcept.afbeelding'),
                       afbeelding_node))
                g.add((afbeelding_node,
                       URIRef(
                           'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcDocument.bestandsnaam'),
                       Literal(image_filename)))
                g.add((afbeelding_node,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcDocument.uri'),
                       image_uri))
                g.add((afbeelding_node,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcDocument.mimeType'),
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAlgMimeType/image-png')))

            else:
                self.bord_register_not_found.add(bord.code)

    def add_borden_register(self, bord_register: Path):
        with open(bord_register, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            for row in reader:
                splitted = row[1].split('. ', 2)
                self.bord_register[splitted[0]] = {'image': row[0], 'betekenis': splitted[1]}
