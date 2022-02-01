from src.clean_function import *
from src.predict_function import *
from typing import Dict
from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from pydantic import BaseModel
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
import os
import time
import shutil


class PetDetail(BaseModel):
    species: str
    bact_species: str
    vitek_id: str
    submitted_sample: str
    sir: Dict


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

app = FastAPI()
prediction = predictor()
conn = sqlalchemy.create_engine(
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/antimicrobial_system")

# @app.get("/")
# async def root():
#     return {"message": "Hello World"}


@app.post("/api/predict/")
async def predict(petDetail: PetDetail):
    start_time = time.time()

    species = cleanSpecies(petDetail.species)
    bact_genus = cleanBactGenus(petDetail.bact_species)
    submitted_sample = cleanSubmittedSample(
        petDetail.submitted_sample, petDetail.vitek_id)
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

    end_time = time.time()

    return {'answer': result, 'processing_time': (end_time - start_time)}


@app.get("/api/species/")
async def species():
    species = pd.read_sql("SELECT id , name FROM public.species", conn)
    return {
        "status": "success",
        "data": {
            "species": [{
                'id': row[0],
                'name': row[1]
            } for row in species.values]
        }
    }


@app.get("/api/vitek_id/")
async def vitek_id():
    vitek_id = pd.read_sql("SELECT id , name FROM public.vitek_id_card", conn)
    return {
        "status": "success",
        "data": {
            "vitek_id": [
                {
                    'id': row[0],
                    'name': row[1]
                } for row in vitek_id.values
            ]
        }
    }


@app.get("/api/antimicrobial_sir/")
async def antimicrobial_sir(v_id):
    query = sqlalchemy.text(
        "SELECT id , name , sir_type_id FROM public.antimicrobial_sir WHERE vitek_id = :v_id")
    antimicrobial_sir = pd.read_sql_query(query, conn, params={"v_id": v_id})

    query = sqlalchemy.text(
        "SELECT id , name FROM public.sir_type WHERE id IN :sir_type")
    sir_type = pd.read_sql_query(
        query, con=conn, params={"sir_type": tuple(int(i) for i in antimicrobial_sir["sir_type_id"].unique())})

    query = sqlalchemy.text(
        "SELECT id , sir_type_id ,  symbol FROM public.sir_sub_type WHERE sir_type_id IN :sir_sub_type")
    sir_sub_type = pd.read_sql_query(
        query, con=conn, params={"sir_sub_type": tuple(int(i) for i in antimicrobial_sir["sir_type_id"].unique())})

    return {
        "status": "success",
        "data": {
            "antimicrobial": [
                {
                    "id": row[0],
                    "name": row[1],
                    "sir_type": row[2]
                } for row in antimicrobial_sir.values],
            "sir_type": [
                {
                    "id": row[0],
                    "name": row[1],
                    "sub_type": [
                        {
                            "id": sub_row[0],
                            "name": sub_row[2]
                        }
                        for sub_row in sir_sub_type.values if sub_row[1] == row[0]
                    ]
                } for row in sir_type.values]
        }
    }

@app.post("/api/upload/")
async def upload(vitek_id, background_tasks: BackgroundTasks, in_file: UploadFile = File(...),):
    with open(f"./upload_file/{hash(time.time())}_{in_file.filename}", mode="wb") as out_file:
        shutil.copyfileobj(in_file.file, out_file)
    
    return {
        "status": "success",
        "data":
        {
            "filename": "",
            "start_date": ""
        }
    }
