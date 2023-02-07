import dataclasses


@dataclasses.dataclass
class WDBOphanging:
    id: int = -1
    client_id: str = ''
    lengte: int = -1
    diameter: int = -1
    opstelling_id: int = -1
    sokkel_naam: str = ''
    kleur: str = ''
    ondergrond: str = ''

