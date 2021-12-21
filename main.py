from src.clean_function import *
from typing import Dict
from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np
import pandas as pd

class PetDetail(BaseModel):
    species: str
    bact_species: str
    vitek_id: str
    submitted_sample: str
    sir: Dict

# load model
gpModel = {
            "amikacin" : joblib.load('./ml_model/GP/amikacin.joblib'),
            "amoxicillin/clavulanic acid" : joblib.load('./ml_model/GP/amoxicillin_clavulanic acid.joblib'),
            "cefalexin" : joblib.load('./ml_model/GP/cefalexin.joblib'),
            "cefovecin" : joblib.load('./ml_model/GP/cefovecin.joblib'),
            "clindamycin" : joblib.load('./ml_model/GP/clindamycin.joblib'),
            "doxycycline" : joblib.load('./ml_model/GP/doxycycline.joblib'),
            "enrofloxacin" : joblib.load('./ml_model/GP/enrofloxacin.joblib'),
            "marbofloxacin" : joblib.load('./ml_model/GP/marbofloxacin.joblib'),
            "nitrofurantoin" : joblib.load('./ml_model/GP/nitrofurantoin.joblib'),
            "trimethoprim_sulfamethoxazole" : joblib.load('./ml_model/GP/trimethoprim_sulfamethoxazole.joblib'),
            "vancomycin" : joblib.load('./ml_model/GP/vancomycin.joblib')
}
gnModel = {
            "amikacin" : joblib.load('./ml_model/GN/amikacin.joblib'),
            "amoxicillin/clavulanic acid" : joblib.load('./ml_model/GN/amoxicillin_clavulanic acid.joblib'),
            "cefalexin" : joblib.load('./ml_model/GN/cefalexin.joblib'),
            "cefovecin" : joblib.load('./ml_model/GN/cefovecin.joblib'),
            "doxycycline" : joblib.load('./ml_model/GN/doxycycline.joblib'),
            "enrofloxacin" : joblib.load('./ml_model/GN/enrofloxacin.joblib'),
            "gentamicin" : joblib.load('./ml_model/GN/gentamicin.joblib'),
            "imipenem" : joblib.load('./ml_model/GN/imipenem.joblib'),
            "marbofloxacin" : joblib.load('./ml_model/GN/marbofloxacin.joblib'),
            "nitrofurantoin" : joblib.load('./ml_model/GN/nitrofurantoin.joblib'),
            "trimethoprim_sulfamethoxazole" : joblib.load('./ml_model/GN/trimethoprim_sulfamethoxazole.joblib')
}

# read schema
schema_gp_file = open("./ml_model/schema/schema_gp.txt", "r")
schema_gp = schema_gp_file.read()
schema_gp_list = [e.strip() for e in schema_gp.replace("\n", "").replace("'", "").split(",")]
schema_gp_file.close()

schema_gn_file = open("./ml_model/schema/schema_gn.txt", "r")
schema_gn = schema_gn_file.read()
schema_gn_list = [e.strip() for e in schema_gn.replace("\n", "").replace("'", "").split(",")]
schema_gn_file.close()


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
    vitek_id = petDetail.vitek_id.upper().strip()
    
    req = dict()
    req['species'] = [species]
    req['bact_genus'] = [bact_genus]
    req['submitted_sample'] = [submitted_sample]
    req['vitek_id'] = [vitek_id]
    for key, value in petDetail.sir.items():
        req[f'S/I/R_{key.lower()}'] = [cleanSIR(value)]
        
    # one hot encoding
    req_dummy = pd.get_dummies(pd.DataFrame(req))

    # predict answer
    answer = []
    if vitek_id == 'GP':          
        X_predict = pd.DataFrame(columns=schema_gp_list)
        X_predict = X_predict.append(req_dummy).loc[:, :schema_gp_list[-1]]
        X_predict.fillna(0, inplace=True)
        
        for drugName, model in gpModel.items():
            if model.predict(X_predict)[0] == True:
                answer.append(drugName)
                
    elif vitek_id == 'GN':
        X_predict = pd.DataFrame(columns=schema_gn_list)
        X_predict = X_predict.append(req_dummy).loc[:, :schema_gn_list[-1]]
        X_predict.fillna(0, inplace=True)
        
        for drugName, model in gnModel.items():
            if model.predict(X_predict)[0] == True:
                answer.append(drugName)
    
    return {'answer': answer}
