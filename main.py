from importlib.resources import path
from typing import Dict
from fastapi import FastAPI, File, UploadFile, BackgroundTasks
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
import os
import time
import datetime
import shutil
import asyncio
from src.model import PetDetail
from src.predictor import Predictior
from src.csv_validation import csv_validation


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

app = FastAPI()
conn = sqlalchemy.create_engine(
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/antimicrobial_system")

# prediction = predictor()

predictor = Predictior(conn)


@app.get("/api/species/")
async def species():
    species = pd.read_sql_query("SELECT id , name FROM public.species", conn)
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
    vitek_id = pd.read_sql_query(
        "SELECT id , name FROM public.vitek_id_card", conn)
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


@app.get("/api/bacteria_genus/")
async def bacteria_genus():
    bacteria_genus = pd.read_sql_query(
        "SELECT id , name FROM public.bacteria_genus", conn)
    return {
        "status": "success",
        "data": {
            "bacteria_genus": [
                {
                    'id': row[0],
                    'name': row[1]
                } for row in bacteria_genus.values
            ]
        }
    }


@app.get("/api/submitted_sample/")
async def submitted_sample():
    submitted_sample_binning_latest = pd.read_sql_query(
        """
        SELECT public.submitted_sample_binning_model_group.schema
	    FROM public.submitted_sample_binning_model_group
	    INNER JOIN public.model_group ON public.model_group.id = public.submitted_sample_binning_model_group.model_group_id
	    WHERE (public.model_group.vitek_id , public.model_group.version) IN (
		    SELECT vitek_id , MAX(version) 
		    FROM public.model_group
		    GROUP BY vitek_id);
        """, conn)

    submitted_all = set()
    for sub_sam in submitted_sample_binning_latest.values:
        submitted_all.update(set(eval(sub_sam[0])))

    query = sqlalchemy.text(
        "SELECT id , name FROM public.submitted_sample WHERE name IN :sub_all")

    submitted_sample = pd.read_sql_query(
        query, conn, params={'sub_all': tuple(submitted_all)})

    return {
        "status": "success",
        "data": {
            "submitted_sample": [
                {
                    'id': row[0],
                    'name': row[1]
                } for row in submitted_sample.values
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


@app.post("/api/predict/")
async def predict(petDetail: PetDetail):
    species = petDetail.species.lower().strip()
    bact_genus = petDetail.bact_genus.lower().strip()
    submitted_sample = petDetail.submitted_sample.lower().strip()
    vitek_id = petDetail.vitek_id.upper().strip()

    data = dict()
    data['species'] = species
    data['bact_genus'] = bact_genus
    data['submitted_sample'] = submitted_sample
    data['vitek_id'] = vitek_id
    for key, value in petDetail.sir.items():
        value = value.upper()
        data[f'S/I/R_{key.lower().strip()}'] = {'POS': '+',
                                                'NEG': '-'}.get(value, value)

    v_id = {"GN": 0, "GP": 1}.get(vitek_id, -1)

    # predict answer
    if v_id != -1:
        result = predictor.predict(pd.Series(data), v_id)
        return {
            "status": "success",
            "data":
            {
                "answers": result
            }
        }
    else:
        return {
            "status": "failed",
            "message": "vitek_id must have GN or GP only."
        }


@app.post("/api/upload/")
async def upload(vitek_id, background_tasks: BackgroundTasks, in_file: UploadFile = File(...),):
    start_time = time.time()
    upload_date = datetime.datetime.now()
    with open(f"./upload_file/{hash(time.time())}_{in_file.filename}", mode="wb") as out_file:
        shutil.copyfileobj(in_file.file, out_file)
    with conn.connect() as con:
        query = sqlalchemy.text(
            "INSERT INTO public.upload_file_log(filename, start_date,status) VALUES (:filename, :date, 'pending') RETURNING id;")
        rs = con.execute(query, filename=in_file.filename, date=upload_date)
        for row in rs:
            id_upload = row[0]
    background_tasks.add_task(
        csv_validation, id_upload, in_file.filename, vitek_id, path)
    return {
        "status": "success",
        "data":
        {
            "filename": in_file.filename,
            "start_date": upload_date,
            "time": time.time() - start_time
        }
    }


@app.post("/api/logs_upload/")
async def logs_upload():
    pass
    # upload_file_log = pd.read_sql(
    #     "SELECT id , name FROM public.upload_file_log", conn)
    # return {
    #     "status": "success",
    #     "data":
    #     {
    #     }
    # }
