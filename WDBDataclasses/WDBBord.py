import dataclasses


@dataclasses.dataclass
class WDBBord:
    id: int = -1
    hoek: float = -1.0
    opstelling_id: int = -1
    y: int = -1
    parameters: str = ''
    code: str = ''
    folie_type: str = ''
    vorm: str = ''
    breedte: int = -1
    hoogte: int = -1
