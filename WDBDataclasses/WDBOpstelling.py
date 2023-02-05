import dataclasses


@dataclasses.dataclass
class WDBOpstelling:
    id: int = -1
    zijde_van_de_rijweg: str = ''
    status: str = ''
    wegsegment_id: int = -1
