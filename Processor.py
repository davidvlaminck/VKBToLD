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

    def process(self, batch_size: int = 100, write_size: int = 14000):
        g = self.write_and_create_graph(None)

        opstelling_ids = []
        for write_count, opstelling in enumerate(self.executor.get_all_opstellingen()):
            opstelling_ids.append(opstelling.id)
            self.process_opstelling(g, opstelling)

            if len(opstelling_ids) >= batch_size:
                self.process_borden(g, opstelling_ids)
                self.process_ophangingen(g, opstelling_ids)
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

        # TODO beheerder

        if opstelling.wegsegment_id is not None:
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
                raise ValueError(f"Verkeersbordopstelling.positieTovRijweg can't be mapped to it: "
                                 f"{opstelling.zijde_van_de_rijweg}")

    def process_ophangingen(self, g: Graph, opstelling_ids: [int]):
        ophanging_ids = []
        for ophanging in self.executor.get_all_ophangingen(opstelling_ids):
            if ophanging.id is None:
                continue
            ophanging_ids.append(ophanging.id)
            self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/ophanging_{ophanging.id}')
            opstelling_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/opstelling_{ophanging.opstelling_id}')
            if 'steun' in ophanging.client_id:
                g.add((self_uri, RDF.type,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Verkeersbordsteun')))
                g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Verkeersbordsteun.type'),
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlVerkeersbordsteunType/rechte-paal')))
            else:
                raise ValueError(f"can't create a type for this ophanging: {ophanging}")

            # hoortbij relatie naar opstelling
            relatie_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/ophanging_{ophanging.id}-opstelling_{ophanging.opstelling_id}')
            g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij')))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
                   self_uri))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
                   opstelling_uri))

            if ophanging.lengte is not None and ophanging.lengte > -1:
                lengte_node = BNode()
                g.add((self_uri,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Verkeersbordsteun.lengte'),
                       lengte_node))
                g.add((lengte_node,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMeter.waarde'),
                       Literal(ophanging.lengte / 1000.0, datatype=XSD.decimal)))

            if ophanging.diameter is not None and ophanging.diameter > -1:
                diameter_node = BNode()
                g.add((self_uri,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Verkeersbordsteun.diameter'),
                       diameter_node))
                g.add((diameter_node,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                       Literal(ophanging.diameter * 1.0, datatype=XSD.decimal)))

            fundering_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/fundering_{ophanging.id}')
            g.add((fundering_uri, RDF.type,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief')))

            # Bevestiging relatie naar fundering
            relatie_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/ophanging_{ophanging.id}-fundering_{ophanging.id}')
            g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
                   self_uri))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
                   fundering_uri))

            self.add_afmetingen_to_fundering(g=g, self_uri=fundering_uri, sokkel_naam=ophanging.sokkel_naam)

        self.process_beugels(g=g, ophanging_ids=ophanging_ids)

    def process_beugels(self, g: Graph, ophanging_ids: [int]):
        for beugel in self.executor.get_all_beugels(ophanging_ids):
            if beugel.id is None:
                continue

            self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/beugel_{beugel.id}')
            ophanging_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/ophanging_{beugel.ophanging_id}')
            bord_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/bord_{beugel.bord_id}')

            g.add((self_uri, RDF.type,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestigingsbeugel')))

            # Bevestiging relatie naar ophanging
            relatie_uri = URIRef(
                f'https://data.awvvlaanderen.be/id/asset/beugel_{beugel.id}-ophanging_{beugel.ophanging_id}')
            g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
                   self_uri))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
                   ophanging_uri))

            # Bevestiging relatie naar bord
            relatie_uri = URIRef(
                f'https://data.awvvlaanderen.be/id/asset/beugel_{beugel.id}-bord_{beugel.bord_id}')
            g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
                   self_uri))
            g.add((relatie_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
                   bord_uri))

    def add_afmetingen_to_fundering(self, g: Graph, self_uri: URIRef, sokkel_naam: str):
        if sokkel_naam is None:
            return

        afmeting_node = BNode()
        vorm_node = BNode()
        kwant_wrd1_node = BNode()
        hoogte_node = BNode()

        if sokkel_naam == '300x300x600, LG-51/VG-51/VG-76':
            g.add(
                (self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.afmetingGrondvlak'),
                 afmeting_node))
            g.add((afmeting_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingGrondvlak.rechthoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxlInCm.breedte'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(30, datatype=XSD.decimal)))
            kwant_wrd2_node = BNode()
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxlInCm.lengte'),
                   kwant_wrd2_node))
            g.add((kwant_wrd2_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(30, datatype=XSD.decimal)))
            g.add(
                (self_uri,
                 URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.funderingshoogte'),
                 hoogte_node))
            g.add((hoogte_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(60, datatype=XSD.decimal)))
        elif sokkel_naam == '400x400x700, LG-76/VG-89':
            g.add(
                (self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.afmetingGrondvlak'),
                 afmeting_node))
            g.add((afmeting_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingGrondvlak.rechthoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxlInCm.breedte'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(40, datatype=XSD.decimal)))
            kwant_wrd2_node = BNode()
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxlInCm.lengte'),
                   kwant_wrd2_node))
            g.add((kwant_wrd2_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(40, datatype=XSD.decimal)))
            g.add(
                (self_uri,
                 URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.funderingshoogte'),
                 hoogte_node))
            g.add((hoogte_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(70, datatype=XSD.decimal)))
        elif sokkel_naam == '500x500x700, LG-89/VG-114':
            g.add(
                (self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.afmetingGrondvlak'),
                 afmeting_node))
            g.add((afmeting_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingGrondvlak.rechthoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxlInCm.breedte'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(50, datatype=XSD.decimal)))
            kwant_wrd2_node = BNode()
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxlInCm.lengte'),
                   kwant_wrd2_node))
            g.add((kwant_wrd2_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(50, datatype=XSD.decimal)))
            g.add(
                (self_uri,
                 URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.funderingshoogte'),
                 hoogte_node))
            g.add((hoogte_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(70, datatype=XSD.decimal)))
        elif sokkel_naam == 'Bodemhuls Ø76':
            g.add(
                (self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Funderingsmassief.afmetingGrondvlak'),
                 afmeting_node))
            g.add((afmeting_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingGrondvlak.rond'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingDiameterInCm.diameter'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(11, datatype=XSD.decimal)))
            g.add((hoogte_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInCentimeter.waarde'),
                   Literal(37, datatype=XSD.decimal)))

    def process_borden(self, g: Graph, opstelling_ids: [int]):
        bord_ids = []
        for bord in self.executor.get_all_borden(opstelling_ids):
            if bord.id is None:
                continue
            bord_ids.append(bord.id)
            self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/bord_{bord.id}')
            opstelling_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/opstelling_{bord.opstelling_id}')

            # TODO onderbord details (relatie tekens)
            # TODO calamiteitenbord details
            if bord.code is not None and bord.code[0] in ['G', 'M']:
                g.add((self_uri, RDF.type,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Onderbord')))
            elif bord.code is not None and bord.code[0:4] == 'ITRS':
                g.add((self_uri, RDF.type,
                       URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#CalamiteitsBord')))
            else:
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

            self.add_afmetingen_to_bord(g=g, self_uri=self_uri, bord=bord)

            self.process_teken(g=g, bord=bord, bord_uri=self_uri)
            self.process_folie(g=g, bord=bord, bord_uri=self_uri)

    def add_afmetingen_to_bord(self, g: Graph, self_uri: URIRef, bord: WDBBord):
        if bord.vorm is None or bord.breedte is None:
            return

        afmeting_node = BNode()
        vorm_node = BNode()
        kwant_wrd1_node = BNode()

        if bord.vorm in ['rh', 'wwr', 'wwl', 'rt'] and bord.breedte is not None:
            g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord'),
                   afmeting_node))
            g.add((afmeting_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord.vierhoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxhInMm.breedte'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                   Literal(bord.breedte, datatype=XSD.decimal)))
            kwant_wrd2_node = BNode()
            g.add((vorm_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingBxhInMm.hoogte'),
                   kwant_wrd2_node))
            g.add((kwant_wrd2_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                   Literal(bord.hoogte, datatype=XSD.decimal)))
        elif bord.vorm in ['dh', 'odh']:
            g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord'),
                   afmeting_node))
            g.add((afmeting_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord.driehoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingZijdeInMm.zijde'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                   Literal(bord.breedte, datatype=XSD.decimal)))
        elif bord.vorm in ['zh']:
            g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord'),
                   afmeting_node))
            g.add((afmeting_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord.zeshoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingZijdeInMm.zijde'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                   Literal(bord.breedte, datatype=XSD.decimal)))
        elif bord.vorm in ['ah']:
            g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord'),
                   afmeting_node))
            g.add((afmeting_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord.achthoekig'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingZijdeInMm.zijde'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                   Literal(bord.breedte, datatype=XSD.decimal)))
        elif bord.vorm in ['ro']:
            g.add((self_uri, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord'),
                   afmeting_node))
            g.add((afmeting_node,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DtuAfmetingVerkeersbord.rond'),
                   vorm_node))
            g.add((vorm_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#DtcAfmetingDiameterInMm.diameter'),
                   kwant_wrd1_node))
            g.add((kwant_wrd1_node,
                   URIRef(
                       'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#KwantWrdInMillimeter.waarde'),
                   Literal(bord.breedte, datatype=XSD.decimal)))
        else:
            raise ValueError(f"bord.vorm can't be mapped: {bord.vorm}")

    def process_folie(self, g: Graph, bord: WDBBord, bord_uri: URIRef):
        self_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/folie_{bord.id}')

        g.add((self_uri, RDF.type,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendeFolie')))

        # Bevestiging relatie
        relatie_uri = URIRef(f'https://data.awvvlaanderen.be/id/asset/bord_{bord.id}-folie_{bord.id}')
        g.add((relatie_uri, RDF.type, URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging')))
        g.add((relatie_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.bron'),
               bord_uri))
        g.add((relatie_uri,
               URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#RelatieObject.doel'),
               self_uri))

        if bord.folie_type == 'Onbekend' or bord.folie_type == '' or bord.folie_type is None or bord.folie_type == 'nvt':
            return
        elif bord.folie_type == '3.a':
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendeFolie.folietype'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlFolieType/folietype-3a')))
        elif bord.folie_type == '3.b':
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendeFolie.folietype'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlFolieType/folietype-3b')))
        elif bord.folie_type == '3':
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendeFolie.folietype'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlFolieType/folietype-3a-en-3b')))
        elif bord.folie_type == '1':
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendeFolie.folietype'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlFolieType/folietype-1')))
        elif bord.folie_type == '2':
            g.add((self_uri,
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#RetroreflecterendeFolie.folietype'),
                   URIRef('https://wegenenverkeer.data.vlaanderen.be/id/concept/KlFolieType/folietype-2')))
        else:
            raise ValueError(f"bord.folie_type can't be mapped to it: {bord.folie_type}")

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
