from fastapi import Depends, FastAPI, File, UploadFile, BackgroundTasks
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy
import os
import time
import datetime
import copy
import shutil
import asyncio
from src.utility import cleanSubmittedSample
from src.model import PetDetail
from src.predictor import Predictior
from src.table_to_csv import TableToCsv
from src.upload_validator import UploadValidator
from src.upload_tranformation import UploadTranformation
from src.model_training import ModelRetraining

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

DB_HOST = os.environ.get("DB_HOST")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

app = FastAPI()
conn = sqlalchemy.create_engine(
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/antimicrobial_system")

predictor = Predictior(conn)


table_csv = {'GN': TableToCsv(conn, 1), 'GP': TableToCsv(conn, 2)}


@app.get("/api/species/")
def species():
    species = pd.read_sql_query(
        "SELECT id , name FROM public.species", conn)
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
def vitek_id():
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
def bacteria_genus():
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
def submitted_sample():
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
def antimicrobial_sir(v_id):
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
def predict(petDetail: PetDetail):

    species = petDetail.species.lower().strip()
    bact_genus = petDetail.bact_genus.lower().strip()
    submitted_sample = cleanSubmittedSample(petDetail.submitted_sample.lower(
    ).strip())
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

# ---------- UPLOAD  ----------


def uploading(vitek_id: int, uploadfile: dict):
    # uploadfile = {"id": id_upload, "filename": in_file.filename,
    #               "filepath": filepath, "start_date": upload_date}
    def upload_result_func(detail: list, type: str, log_id: int):
        res = pd.DataFrame(detail, columns=["detail"])
        res["type"] = type
        res["upload_file_log_id"] = log_id
        res.to_sql('upload_file_result', schema='public',
                   con=conn, if_exists='append', index=False)

    file_upload = pd.read_csv(uploadfile["filepath"])
    # reset index
    file_upload.index = file_upload.index + 2  # start at 1 + header

    vitek = ['GN', 'GP'][vitek_id - 1]
    upload_validator = UploadValidator(table_csv[vitek], vitek_id)
    result = upload_validator.validate(file_upload)
    if result[0]:
        # File Result
        if result[1] == "warning":
            upload_result_func(result[2], result[1], uploadfile["id"])

        # File
        with conn.connect() as con:
            query = sqlalchemy.text(
                "INSERT INTO public.file(name, upload_at,active,vitek_id) VALUES (:name, :date, true , :v_id) RETURNING id;")
            rs = con.execute(query, name=uploadfile["filename"],
                             date=uploadfile["start_date"], v_id=vitek_id)
            for row in rs:
                file_id = row[0]

        # Report
        uploader = UploadTranformation(vitek_id, conn)
        try:
            row_count = uploader.upload(file_upload, file_id)

            # Add Row Count File
            with conn.connect() as con:
                query = sqlalchemy.text(
                    "UPDATE public.file SET amount_row = :count WHERE id = :id")
                rs = con.execute(query, count=row_count, id=file_id)

            # Reload Table
            table_csv[vitek].startup()

            status = "success"
        except Exception as ex:
            upload_result_func([str(ex)], result[1], uploadfile["id"])
            row_count = 0
            status = "failed"
    else:
        upload_result_func(result[2], result[1], uploadfile["id"])
        row_count = 0
        status = "failed"

    # Update Filelog
    finish_date = datetime.datetime.now()
    delta_time = (finish_date - uploadfile["start_date"]).seconds
    with conn.connect() as con:
        query = sqlalchemy.text(
            """
            UPDATE public.upload_file_log
            SET finish_date=:f_date, "time"=:time, amount_row=:count, status=:status
            WHERE id = :id""")
        rs = con.execute(query, id=uploadfile["id"], f_date=finish_date,
                         time=delta_time, count=row_count, status=status)

    # Delete Temp File
    os.remove(uploadfile["filepath"])
    # print("finish")


@app.post("/api/upload/")
def upload(vitek_id: int, background_tasks: BackgroundTasks, in_file: UploadFile = File(...)):
    if vitek_id not in pd.read_sql_query("SELECT id FROM public.vitek_id_card", conn).values:
        return {
            "status": "failed",
        }
    start_time = time.time()
    upload_date = datetime.datetime.now()
    filepath = f"./upload_file/{hash(start_time)}_{in_file.filename}"
    with open(filepath, mode="wb") as out_file:
        shutil.copyfileobj(in_file.file, out_file)
    with conn.connect() as con:
        query = sqlalchemy.text(
            "INSERT INTO public.upload_file_log(filename, start_date,status , vitek_id) VALUES (:filename, :date, 'pending' , :vitek_id) RETURNING id;")
        rs = con.execute(query, filename=in_file.filename,
                         date=upload_date, vitek_id=vitek_id)
        for row in rs:
            id_upload = row[0]
    uploadfile = {"id": id_upload, "filename": in_file.filename,
                  "filepath": filepath, "start_date": upload_date}
    background_tasks.add_task(
        uploading, vitek_id, uploadfile)
    return {
        "status": "success",
        "data":
        {
            "filename": in_file.filename,
            "start_date": upload_date,
        }
    }


@app.get("/api/upload_logs/")
def upload_logs(page: int = 1):
    # show list
    AMOUNT_PER_PAGE = 10
    offset = (page - 1) * AMOUNT_PER_PAGE

    if offset < 0:
        return {
            "status": "failed",
        }

    # select count
    with conn.connect() as con:
        rs = con.execute("SELECT COUNT(*) FROM public.upload_file_log ")
        for row in rs:
            count = row[0]

    if count == 0:
        return {
            "status": "success",
            "data":
            {
                "logs": [],
                "total": count
            }
        }

    query = sqlalchemy.text("""
        SELECT up.id , up.filename , up.start_date , up.finish_date , up.time , up.amount_row ,up.status , vi.name AS vitek_id
        FROM public.upload_file_log AS up
        INNER JOIN public.vitek_id_card AS vi ON up.vitek_id = vi.id
        ORDER BY finish_date DESC
        LIMIT :app
        OFFSET :offset
        """)
    upload_file_logs = pd.read_sql(
        query, conn, params={"app": AMOUNT_PER_PAGE, "offset": offset})

    query = sqlalchemy.text("""
        SELECT *
        FROM public.upload_file_result
        WHERE upload_file_log_id IN :log_id
        """)

    upload_file_results = pd.read_sql(
        query, conn, params={"log_id": tuple(upload_file_logs["id"].tolist())})

    logs = []
    upload_file_logs = upload_file_logs.set_index("id")
    for _id in upload_file_logs.index:
        result_fetch = upload_file_results[upload_file_results["upload_file_log_id"] == _id]
        result = dict()
        if len(result_fetch) == 0:
            result["type"] = "success"
            result["detail"] = []
        else:
            result["type"] = result_fetch["type"].iloc[0]
            result["detail"] = result_fetch["detail"].tolist()
        logs.append({
            "id": int(_id),
            "filename": upload_file_logs.loc[_id, "filename"],
            "start_date": upload_file_logs.loc[_id, "start_date"].strftime("%d-%b-%Y %H:%M:%S"),
            "finish_date": upload_file_logs.loc[_id, "finish_date"].strftime("%d-%b-%Y %H:%M:%S") if pd.notna(upload_file_logs.loc[_id, "finish_date"]) else '-',
            "time": int(upload_file_logs.loc[_id, "time"]) if pd.notna(upload_file_logs.loc[_id, "time"]) else '-',
            "amount_row": int(upload_file_logs.loc[_id, "amount_row"]) if pd.notna(upload_file_logs.loc[_id, "amount_row"]) else '-',
            "status": upload_file_logs.loc[_id, "status"],
            "result": result,
            "vitek_id": upload_file_logs.loc[_id, "vitek_id"]
        })

    return {
        "status": "success",
        "data":
        {
            "logs": logs,
            "total": count
        }
    }

# ---------- VIEW FILENAME  ----------


@app.get("/api/view_filename")
def view_filename(model_group_id: int):
    query = sqlalchemy.text("""
        SELECT f.id , f.name , f.upload_at , f.amount_row
        FROM public.file AS f
        INNER JOIN public.model_group_file AS mgf ON f.id = mgf.file_id
        WHERE active AND mgf.model_group_id = :mg_id
        ORDER BY upload_at DESC
        """)

    files_fetch = pd.read_sql_query(query, con=conn, params={
                                    "mg_id": model_group_id})

    files = []
    files_fetch = files_fetch.set_index("id")
    for _id in files_fetch.index:
        files.append({
            "id": int(_id),
            "name": files_fetch.loc[_id, "name"],
            "timestamp": files_fetch.loc[_id, "upload_at"].strftime("%d-%b-%Y %H:%M:%S"),
            "amount_row": int(files_fetch.loc[_id, "amount_row"]),
        })
    return {
        "status": "success",
        "data": {
            "files": files,
        }
    }
# ---------- VIEW FILE  ----------


@app.get("/api/view_all_files/")
def view_all_files(page: int = 1):
    # show list
    AMOUNT_PER_PAGE = 10
    offset = (page - 1) * AMOUNT_PER_PAGE

    if offset < 0:
        return {
            "status": "failed",
        }

    # select count
    with conn.connect() as con:
        rs = con.execute("SELECT COUNT(*) FROM public.file ")
        for row in rs:
            count = row[0]

    query = sqlalchemy.text("""
        SELECT public.file.*, public.vitek_id_card.name as vitek_id_name
        FROM public.file
        INNER JOIN public.vitek_id_card ON public.vitek_id_card.id = public.file.vitek_id
        WHERE active AND amount_row IS NOT NULL
        ORDER BY upload_at DESC
        LIMIT :app
        OFFSET :offset
        """)
    file_logs = pd.read_sql(
        query, conn, params={"app": AMOUNT_PER_PAGE, "offset": offset})

    files = []
    file_logs = file_logs.set_index("id")
    for _id in file_logs.index:
        files.append({
            "id": int(_id),
            "name": file_logs.loc[_id, "name"],
            "vitek_id": file_logs.loc[_id, "vitek_id_name"],
            "upload_at": file_logs.loc[_id, "upload_at"].strftime("%d-%b-%Y %H:%M:%S"),
            "amount_row": int(file_logs.loc[_id, "amount_row"]),
            "can_delete": file_logs.loc[_id, "upload_at"].year > 2021
        })
    return {
        "status": "success",
        "data": {
            "files": files,
            "total_row": count
        }
    }
 
 # ---------- VIEW FILE RETRAINING LOG  ----------
    
@app.get("/api/view_file_retraining_log")
def view_file_retraining_log(retraining_log_id: int):
    query = sqlalchemy.text("""
        SELECT file_id, name, upload_at, amount_row
        FROM public.file_retraining_log AS file_re_log
        INNER JOIN public.file AS file ON file.id = file_re_log.file_id
        WHERE retraining_log_id = :re_log_id
        ORDER BY upload_at DESC
        """)

    files_fetch = pd.read_sql_query(query, con=conn, params={"re_log_id": retraining_log_id})

    files = []
    files_fetch = files_fetch.set_index("file_id")
    for _id in files_fetch.index:
        files.append({
            "id": int(_id),
            "name": files_fetch.loc[_id, "name"],
            "timestamp": files_fetch.loc[_id, "upload_at"].strftime("%d-%b-%Y %H:%M:%S"),
            "amount_row": int(files_fetch.loc[_id, "amount_row"]),
        })
    return {
        "status": "success",
        "data": {
            "files": files,
        }
    }

# ---------- RETRAINING ----------


def training(vitek_id: int, table_report: pd.DataFrame):
    # start training
    start_date = datetime.datetime.now()
    with conn.connect() as con:
        query = sqlalchemy.text(
            """INSERT INTO public.retraining_log(vitek_id, start_date, status)
            VALUES (:vitek_id, :start_date, 'training')
            RETURNING id;
            """)
        rs = con.execute(query, vitek_id=vitek_id, start_date=start_date)
        for row in rs:
            retraining_id = row[0]

    # INSERT file_retraining_log
    with conn.connect() as con:
        query = sqlalchemy.text(
            """INSERT INTO public.file_retraining_log(retraining_log_id, file_id)
            VALUES (:retraining_log_id, :file_id)
            """)
        for file_id in table_report.file_id:
            rs = con.execute(
                query, retraining_log_id=retraining_id, file_id=file_id)

    # Retraining
    model_retraining = ModelRetraining(table_report, vitek_id, conn)
    model_group_id = model_retraining.training()

    # finish training
    finish_date = datetime.datetime.now()
    delta_time = (finish_date - start_date).seconds
    with conn.connect() as con:
        query = sqlalchemy.text(
            """
            UPDATE public.retraining_log
            SET finish_date=:finish_date, time=:time, status=:status, model_group_id=:mg_id
            WHERE id = :id""")
        rs = con.execute(query, finish_date=finish_date, time=delta_time, status="success",
                         mg_id=model_group_id, id=retraining_id)


@app.post("/api/retraining/")
def model_retraining(background_tasks: BackgroundTasks):
    
    # Check training status
    count_training_status = pd.read_sql_query(
        """ SELECT COUNT(*)
            FROM public.retraining_log
            WHERE status = 'training'
        """, conn).values[0][0]

    if count_training_status != 0:
        return {
            "status": "success",
            "data": {
                "status": "fail",
                "message": "ขณะนี้ระบบกำลังเทรนโมเดลอยู่"
            }
        }
        
    # Check upload pending status
    count_pending_status = pd.read_sql_query(
        """ SELECT COUNT(*)
            FROM public.upload_file_log
            WHERE status = 'pending'
        """, conn).values[0][0]

    if count_pending_status != 0:
        return {
            "status": "success",
            "data": {
                "status": "fail",
                "message": "ไม่สามารถเทรนโมเดลได้ เนื่องจากขณะนี้ระบบกำลังอัปโหลดไฟล์อยู่"
            }
        }
    
    # Check last model_group's file
    file_query = sqlalchemy.text(
        """ SELECT file_id
            FROM public.model_group as mg
            INNER JOIN public.model_group_file as mgf ON mgf.model_group_id = mg.id
            WHERE mg.vitek_id = :v_id AND version = (SELECT MAX(version) 
				                                FROM public.model_group
				                                WHERE vitek_id = :v_id)
        """)
    count_training = 0
    table_copy = copy.copy(table_csv)
    for vitek_id in [1, 2]:
        vitek = ["GN", "GP"][vitek_id - 1]
        file_id_list = list(pd.read_sql_query(file_query, conn, params={"v_id": vitek_id})["file_id"].values)

        file_id_list.sort()
        table_copy[vitek].file_id.sort()
        
        if file_id_list == table_copy[vitek].file_id:
            count_training += 1
        else:
            background_tasks.add_task(training, vitek_id, table_copy[vitek])
    
    if count_training >= 2:
        return {
                "status": "success",
                "data": {
                    "status": "fail",
                    "message": "ขณะนี้ยังไม่มีข้อมูลใหม่สำหรับเทรนโมเดล"
                }
            } 
 
    return {
        "status": "success",
        "data": {
            "status": "success",
            "message": "ขณะนี้ระบบได้เริ่มเทรนโมเดลแล้ว"
        }
        
    }

@app.get("/api/retraining_logs/")
def retraining_logs(page: int = 1):
    # show list
    AMOUNT_PER_PAGE = 10
    offset = (page - 1) * AMOUNT_PER_PAGE

    if offset < 0:
        return {
            "status": "failed",
        }

    # select count
    with conn.connect() as con:
        rs = con.execute("SELECT COUNT(*) FROM public.retraining_log")
        for row in rs:
            count = row[0]

    if count == 0:
        return {
            "status": "success",
            "data":
            {
                "logs": [],
                "total": count
            }
        }

    query = sqlalchemy.text("""
        SELECT log.id, log.start_date, log.finish_date, log.time, log.status, mg.version, vi.id AS vitek_id, vi.name AS vitek_name
        FROM public.retraining_log AS log 
        INNER JOIN public.vitek_id_card AS vi ON log.vitek_id = vi.id
        LEFT JOIN public.model_group AS mg ON mg.id = log.model_group_id
        ORDER BY finish_date DESC
        LIMIT :app
        OFFSET :offset
        """)
    retraining_logs = pd.read_sql(
        query, conn, params={"app": AMOUNT_PER_PAGE, "offset": offset})

    logs = []
    retraining_logs = retraining_logs.set_index("id")
    for _id in retraining_logs.index:
        logs.append({
            "id": int(_id),
            "vitek_id": int(retraining_logs.loc[_id, "vitek_id"]),
            "vitek_name": retraining_logs.loc[_id, "vitek_name"],
            "start_date": retraining_logs.loc[_id, "start_date"].strftime("%d-%b-%Y %H:%M:%S"),
            "finish_date": retraining_logs.loc[_id, "finish_date"].strftime("%d-%b-%Y %H:%M:%S") if pd.notna(retraining_logs.loc[_id, "finish_date"]) else '-',
            "time": int(retraining_logs.loc[_id, "time"]) if pd.notna(retraining_logs.loc[_id, "time"]) else '-',
            "status": retraining_logs.loc[_id, "status"],
            "version": int(retraining_logs.loc[_id, "version"]) if pd.notna(retraining_logs.loc[_id, "version"]) else None,
        })

    return {
        "status": "success",
        "data":
        {
            "logs": logs,
            "total": count
        }
    }
    

# ---------- DASHBOARD ----------

@app.get("/api/lastest_version/")
def lastest_version(vitek_id):
    query = sqlalchemy.text(
        """ SELECT MAX(version)
            FROM public.model_group
            WHERE vitek_id = :vitek_id
        """)
    lastest_version = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id})

    return {
        "status": "success",
        "data": {
            "lastest_version": int(lastest_version.values[0][0])
        }
    }


@app.get("/api/antimicrobial_model/")
def antimicrobial_model(vitek_id):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_answer.id, public.antimicrobial_answer.name
            FROM public.model
            INNER JOIN public.model_group_model ON model.id = model_group_model.model_id
            INNER JOIN public.model_group ON public.model_group_model.model_group_id = public.model_group.id
            INNER JOIN public.antimicrobial_answer ON public.model.antimicrobial_id = public.antimicrobial_answer.id
            WHERE public.model_group.vitek_id = :vitek_id
            GROUP BY public.antimicrobial_answer.id, public.antimicrobial_answer.name
            ORDER BY public.antimicrobial_answer.name
        """)
    antimicrobial = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id})

    return {
        "status": "success",
        "data": {
            "antimicrobial": [{
                'id': row[0],
                'name': row[1]
            } for row in antimicrobial.values]
        }
    }

# ---------- DATASET DASHBOARD ----------


@app.get("/api/dashboard_case/")
def dashboard_case(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT to_char(public.report.report_issued_date, 'YYYY-MM'), COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY to_char(public.report.report_issued_date, 'YYYY-MM')
            ORDER BY 1
        """)
    case = pd.read_sql_query(query, conn, params={
                             "vitek_id": vitek_id, "version": version})

    return {
        "status": "success",
        "data": {
            "cases": [{
                'date': row[0],
                'count': row[1]
            } for row in case.values]
        }
    }


@app.get("/api/dashboard_species/")
def dashboard_species(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.species.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.species ON public.report.species_id = public.species.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.species.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    species = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id, "version": version})

    return {
        "status": "success",
        "data": {
            "species": [{
                'name': row[0],
                'count': row[1]
            } for row in species.values]
        }
    }


@app.get("/api/dashboard_bacteria_genus/")
def dashboard_bacteria_genus(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.bacteria_genus.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.bacteria_genus ON public.report.bacteria_genus_id = public.bacteria_genus.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.bacteria_genus.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    bacteria_genus = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id, "version": version})[:10]

    return {
        "status": "success",
        "data": {
            "bacteria_genus": [{
                'name': row[0],
                'count': row[1]
            } for row in bacteria_genus.values]
        }
    }


@app.get("/api/dashboard_submitted_sample/")
def dashboard_submitted_sample(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.submitted_sample.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.submitted_sample ON public.report.submitted_sample_id = public.submitted_sample.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.submitted_sample.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    submitted_sample = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id, "version": version})[:10]

    return {
        "status": "success",
        "data": {
            "submitted_sample": [{
                'name': row[0],
                'count': row[1]
            } for row in submitted_sample.values]
        }
    }


@app.get("/api/dashboard_antimicrobial_sir/")
def dashboard_antimicrobial_answer(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_sir.name,
	            COUNT(CASE WHEN public.sir_sub_type.id=1 THEN 1 END) as "POS",
	            COUNT(CASE WHEN public.sir_sub_type.id=2 THEN 1 END) as "NEG",
 	            COUNT(CASE WHEN public.sir_sub_type.id=3 THEN 1 END) as "S",
	            COUNT(CASE WHEN public.sir_sub_type.id=4 THEN 1 END) as "I",
	            COUNT(CASE WHEN public.sir_sub_type.id=5 THEN 1 END) as "R"
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.report_sir ON public.report.id = public.report_sir.report_id
            INNER JOIN public.antimicrobial_sir ON public.report_sir.antimicrobial_id = public.antimicrobial_sir.id
            INNER JOIN public.sir_sub_type ON public.report_sir.sir_sub_id = public.sir_sub_type.id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.antimicrobial_sir.name
            ORDER BY public.antimicrobial_sir.name
        """)
    antimicrobial_sir = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id, "version": version})

    return {
        "status": "success",
        "data": {
            "antimicrobial_sir": [{
                'name': row[0],
                'count': {
                    'pos': row[1],
                    'neg': row[2],
                    's': row[3],
                    'i': row[4],
                    'r': row[5]
                }
            } for row in antimicrobial_sir.values]
        }
    }


@app.get("/api/dashboard_antimicrobial_answer/")
def dashboard_antimicrobial_answer(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_answer.name, COUNT(public.report.id)
            FROM public.model_group
            INNER JOIN public.model_group_file ON public.model_group.id = public.model_group_file.model_group_id
            INNER JOIN public.report ON public.model_group_file.file_id = public.report.file_id
            INNER JOIN public.report_answer ON public.report.id = public.report_answer.report_id
            INNER JOIN public.antimicrobial_answer ON antimicrobial_answer.id = public.report_answer.antimicrobial_id
            WHERE public.model_group.vitek_id = :vitek_id AND public.model_group.version = :version
            GROUP BY public.antimicrobial_answer.name
            ORDER BY COUNT(public.report.id) DESC
        """)
    antimicrobial_answer = pd.read_sql_query(
        query, conn, params={"vitek_id": vitek_id, "version": version})[:11]

    return {
        "status": "success",
        "data": {
            "antimicrobial_answers": [{
                'name': row[0],
                'count': row[1]
            } for row in antimicrobial_answer.values]
        }
    }

# ---------- PERFORMANCE DASHBOARD ----------


@app.get("/api/dashboard_performance_by_antimicrobial/")
def dashboard_performance_by_antimicrobial(antimicrobial_id):
    query = sqlalchemy.text(
        """ SELECT mg.version, m.accuracy, m.precision, m.recall, m.f1
            FROM public.model as m
            INNER JOIN public.model_group_model ON m.id = public.model_group_model.model_id
            INNER JOIN public.model_group AS mg ON public.model_group_model.model_group_id =  mg.id
            WHERE m.antimicrobial_id = :antimicrobial_id AND mg.version > 0
        """)
    performance = pd.read_sql_query(
        query, conn, params={"antimicrobial_id": antimicrobial_id})

    return {
        "status": "success",
        "data": {
            "performances": [{
                'version': row[0],
                'accuracy': row[1],
                'precision': row[2],
                'recall': row[3],
                'f1': row[4]
            } for row in performance.values]
        }
    }


@app.get("/api/dashboard_performance_by_version/")
def dashboard_performance_by_version(vitek_id, version):
    query = sqlalchemy.text(
        """ SELECT public.antimicrobial_answer.name, m_group.version, m.accuracy, m.precision, m.recall, m.f1, m.performance, m_group.model_group_id
            FROM public.model_group as mg
            INNER JOIN public.model_group_model as mgm ON mg.id = mgm.model_group_id
            INNER JOIN public.model as m ON mgm.model_id = m.id
            INNER JOIN public.antimicrobial_answer ON m.antimicrobial_id = public.antimicrobial_answer.id
            INNER JOIN (
            	SELECT *
            	FROM public.model_group_model
            	INNER JOIN public.model_group ON public.model_group_model.model_group_id = public.model_group.id
             	WHERE public.model_group.version > 0
            	) as m_group ON m.id = m_group.model_id
            WHERE mg.vitek_id = :vitek_id AND mg.version = :version
            ORDER BY public.antimicrobial_answer.name
        """)

    query_test_by_case = sqlalchemy.text(
        """ SELECT 'All Model (Test By Case)' as name, mg.version, mg.accuracy, mg.precision, mg.recall, mg.f1, '-' as performance, mg.id as model_group_id
            FROM public.model_group as mg
            WHERE mg.vitek_id = :vitek_id AND mg.version = :version
        """
    )

    params = {"vitek_id": vitek_id, "version": version}
    performance = pd.read_sql_query(query, conn, params=params)
    test_by_case = pd.read_sql_query(query_test_by_case, conn, params=params)
    performance = performance.append(test_by_case)

    return {
        "status": "success",
        "data": {
            "performances": [{
                'antimicrobial': row[0],
                'version': row[1],
                'accuracy': row[2],
                'precision': row[3],
                'recall': row[4],
                'f1': row[5],
                'performance': row[6],
                'model_group_id': row[7]
            } for row in performance.values]
        }
    }

# ---------- CONFIGURATION ----------


@app.get("/api/configuration_xgb_parameter")
def configuration_xgb_parameter(vitek_id):
    query = sqlalchemy.text("""
    SELECT anti.name , n_estimators, gamma, max_depth, subsample, colsample_bytree, learning_rate, algorithm,  random_state
    FROM public.model_configuration AS mc
    INNER JOIN public.antimicrobial_answer AS anti ON anti.id = mc.antimicrobial_id
    WHERE vitek_id = :v_id;""")
    configs = pd.read_sql_query(query, conn, params={"v_id": vitek_id})
    xgb_params = {}

    def to_params(params: pd.Series):
        indexes = params.index.tolist()
        indexes.pop(0)
        param_str = ", ".join(
            [f"{item[0]} = {item[1]}" for item in params[indexes].items()])
        return {
            "name": params["name"],
            "params": param_str
        }

    return {
        "status": "success",
        "data": {
            "xgb_params": configs.apply(to_params, axis=1).values.tolist()}
    }


@app.get("/api/configuration_smote")
def configuration_smote(vitek_id):
    query = sqlalchemy.text("""
    SELECT anti.name , smote , smote_random_state
    FROM public.model_configuration AS mc
    INNER JOIN public.antimicrobial_answer AS anti ON anti.id = mc.antimicrobial_id
    WHERE vitek_id = :v_id;""")
    configs = pd.read_sql_query(query, conn, params={"v_id": vitek_id})
    xgb_params = {}

    def to_params(params: pd.Series):
        return {
            "name": params["name"],
            "algo": params["smote"]
        }

    return {
        "status": "success",
        "data": {
            "smote": configs.apply(to_params, axis=1).values.tolist()}
    }

# ---------- DELETE FILE  ----------


def delete_file_db(file_id: int):
    with conn.connect() as con:
        query = sqlalchemy.text(
            "UPDATE public.file SET active=False WHERE id = :id;")
        con.execute(query, id=file_id)


@app.delete("/api/delete_file")
def delete_file(file_id: int, background_tasks: BackgroundTasks):
    files = pd.read_sql_query("SELECT id FROM public.file", conn)
    if file_id not in files["id"].values:
        return {
            "status": "failed",
        }
    files_used = pd.read_sql_query(
        "SELECT DISTINCT file_id FROM public.model_group_file", conn)
    if file_id not in files_used['file_id'].values:
        # DELETE FILE IN DATABASE
        with conn.connect() as con:
            query = sqlalchemy.text("DELETE FROM public.file WHERE id = :id;")
            con.execute(query, id=file_id)
    else:
        # DELETE FILE SET STATUS
        background_tasks.add_task(delete_file_db, file_id=file_id)

    return {
        "status": "success",
    }


