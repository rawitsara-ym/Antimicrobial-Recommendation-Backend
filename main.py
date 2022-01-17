from src.clean_function import *
from src.predict_function import *
from typing import Dict
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd

# import time

class PetDetail(BaseModel):
    species: str
    bact_species: str
    vitek_id: str
    submitted_sample: str
    sir: Dict


app = FastAPI()
prediction = predictor()


# @app.get("/")
# async def root():
#     return {"message": "Hello World"}

@app.post("/api/predict/")
async def predict(petDetail: PetDetail):
    # start_time = time.time()

    species = cleanSpecies(petDetail.species)
    bact_genus = cleanBactGenus(petDetail.bact_species)
    submitted_sample = cleanSubmittedSample(petDetail.submitted_sample, petDetail.vitek_id)
    vitek_id = petDetail.vitek_id.upper().strip()
    
    data = dict()
    data['species'] = species
    data['bact_genus'] = bact_genus
    data['submitted_sample'] = submitted_sample
    data['vitek_id'] = vitek_id
    for key, value in petDetail.sir.items():
        data[f'S/I/R_{key.lower()}'] = cleanSIR(value)
     
    # predict answer   
    result = prediction(pd.Series(data))
    
    # end_time = time.time()

    # return {'answer': result,'processing_time' : (end_time - start_time)}
    return {'answer': result}
