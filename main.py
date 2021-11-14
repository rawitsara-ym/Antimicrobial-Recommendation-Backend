from src.clean_function import *
from typing import Dict
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np

class PetDetail(BaseModel):
    species: str
    bact_species: str
    vitek_id: str
    submitted_sample: str
    sir: Dict

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

# test clean function
@app.post("/model/")
async def predict(petDetail: PetDetail):
    species = cleanSpecies(petDetail.species)
    bact_genus = cleanBactGenus(petDetail.bact_species)
    submitted_sample = cleanSubmittedSample(petDetail.submitted_sample, petDetail.vitek_id)
    sir = dict()
    for key, value in petDetail.sir.items():
        if cleanSIR(value) is np.nan:
            sir[key] = ''
        else:
            sir[key] = cleanSIR(value)
    print(sir)
    
    return {"species": species, "bact_genus": bact_genus, "submitted_sample": submitted_sample, "sir": sir}