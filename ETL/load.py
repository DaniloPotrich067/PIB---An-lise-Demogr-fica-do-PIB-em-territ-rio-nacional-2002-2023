from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Iterable


def make_engine(user, password, host, port, database, sslmode="require") -> Engine:
    url = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        f"?sslmode={sslmode}"
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)


def make_engine_from_secrets(secrets) -> Engine:
    """Atalho para usar direto com st.secrets['connections']['pib']."""
    cfg = secrets
    return make_engine(
        user=cfg["username"],
        password=cfg["password"],
        host=cfg["host"],
        port=int(cfg["port"]),
        database=cfg["database"],
        sslmode=cfg.get("sslmode", "require"),
    )


def _chunked(rows: list[dict], chunk_size: int) -> Iterable[list[dict]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i: i + chunk_size]


def upsert_df(engine, table_name, df, conflict_cols, update_cols=None, chunk_size=5000) -> int:
    if df is None or df.empty:
        return 0

    rows = df.to_dict(orient="records")
    total = 0

    with engine.begin() as conn:
        from sqlalchemy import MetaData, Table
        meta = MetaData()
        table = Table(table_name, meta, autoload_with=conn)

        for batch in _chunked(rows, chunk_size):
            stmt = pg_insert(table).values(batch)
            if update_cols:
                set_map = {c: getattr(stmt.excluded, c) for c in update_cols}
                stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=set_map)
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

            res = conn.execute(stmt)
            total += res.rowcount if res.rowcount is not None else 0
    return total


def load_all(engine: Engine, pack: dict, reset: bool = False) -> None:
    if reset:
        with engine.begin() as conn:
            conn.execute(text(
                "TRUNCATE fato_indicador_municipio, dim_municipio, "
                "dim_uf, dim_regiao, dim_variavel RESTART IDENTITY CASCADE;"
            ))

    upsert_df(engine, "dim_regiao",    pack["df_regioes"],    ["id_regiao"],    ["sigla_regiao", "nome_regiao"])
    upsert_df(engine, "dim_uf",        pack["df_ufs"],        ["id_uf"],        ["sigla_uf", "nome_uf", "id_regiao"])
    upsert_df(engine, "dim_municipio", pack["df_municipios"], ["id_municipio"], ["nome_municipio", "id_uf"])

    df_var = pack["df_pib"][["nome_variavel", "unidade"]].drop_duplicates()
    upsert_df(engine, "dim_variavel", df_var, ["nome_variavel"], ["unidade"])

    df_var_db = pd.read_sql("SELECT id_variavel, nome_variavel FROM dim_variavel", engine)
    df_fato = pack["df_pib"].merge(df_var_db, on="nome_variavel", how="inner")
    df_fato = df_fato[["id_municipio", "ano", "id_variavel", "valor"]].dropna()
    df_fato[["id_municipio", "ano", "id_variavel"]] = (
        df_fato[["id_municipio", "ano", "id_variavel"]].astype(int)
    )

    upsert_df(
        engine, "fato_indicador_municipio", df_fato,
        ["id_municipio", "ano", "id_variavel"], ["valor"], chunk_size=15000,
    )


def sanity(engine: Engine) -> pd.DataFrame:
    q = """
    SELECT
        (SELECT count(*) FROM dim_municipio)            AS cidades,
        (SELECT count(*) FROM dim_variavel)             AS variaveis,
        (SELECT count(*) FROM fato_indicador_municipio) AS registros_pib
    """
    return pd.read_sql(q, engine)
