from sqlalchemy.engine import Engine
import sqlalchemy


def check_retraining_status(retraining_id: int, conn: Engine):
    with conn.connect() as con:
        query = sqlalchemy.text(
            """
            SELECT status
            FROM public.retraining_log
            WHERE id = :id
            """)
        con.execute(query, id=retraining_id)
        rs = con.execute(query, id=retraining_id)
        for row in rs:
            cancel = row[0]
    return cancel == 'canceling'
