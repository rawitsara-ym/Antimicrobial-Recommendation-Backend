from typing import Dict
from pydantic import BaseModel

class PetDetail(BaseModel):
    species: str
    bact_genus: str
    vitek_id: str
    submitted_sample: str
    sir: Dict