from __future__ import annotations
import json
import re
from typing import Any, Dict
import pandas as pd

# =====================================================
# IO & UTILS
# =====================================================

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def df_from_records(path: str) -> pd.DataFrame:
    data = read_json(path)
    if isinstance(data, list):
        return pd.DataFrame(data)
    raise ValueError(f"JSON em {path} inválido: esperado lista.")

def _safe_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

# =====================================================
# NORMALIZAÇÃO SIDRA
# =====================================================

_SIDRA_MISSING = {"...", "..", "-", "X", "x", "", None}

def parse_sidra_number(series: pd.Series) -> pd.Series:
    """Versão vetorizada para converter strings do SIDRA em float."""
    s = series.astype(str).str.strip()
    # Substitui marcadores de ausência por NaN
    s = s.replace(list(_SIDRA_MISSING), pd.NA)
    # Limpa pontuação brasileira (1.234,56 -> 1234.56)
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def split_municipio_uf_vectorized(series: pd.Series) -> pd.DataFrame:
    """Extrai Nome e UF da string 'Nome do Municipio - UF'."""
    # Regex para capturar tudo antes do último ' - XX'
    extracted = series.str.extract(r"^(.*)\s-\s([A-Z]{2})$")
    extracted.columns = ["municipio_nome_hint", "uf_sigla_hint"]
    return extracted.fillna("")

# =====================================================
# TRANSFORMERS (LOCALIDADES)
# =====================================================

def transform_regioes(path: str) -> pd.DataFrame:
    df = df_from_records(path)
    df = df.rename(columns={"id": "id_regiao", "sigla": "sigla_regiao", "nome": "nome_regiao"})
    df["id_regiao"] = _safe_int(df["id_regiao"]).dropna().astype(int)
    return df[["id_regiao", "sigla_regiao", "nome_regiao"]].drop_duplicates()

def transform_ufs(path: str) -> pd.DataFrame:
    df = df_from_records(path)
    # Flatten do dicionário 'regiao' dentro do JSON de UFs
    reg_exp = pd.json_normalize(df["regiao"]).add_prefix("regiao_")
    df = pd.concat([df.drop(columns=["regiao"]), reg_exp], axis=1)
    
    df = df.rename(columns={
        "id": "id_uf", "sigla": "sigla_uf", "nome": "nome_uf", "regiao_id": "id_regiao"
    })
    
    for col in ["id_uf", "id_regiao"]:
        df[col] = _safe_int(df[col])
    
    return df.dropna(subset=["id_uf", "id_regiao"])[["id_uf", "sigla_uf", "nome_uf", "id_regiao"]].drop_duplicates()

def transform_municipios(path: str) -> pd.DataFrame:
    raw = read_json(path)
    rows = []
    for item in raw:
        try:
            # Caminho aninhado no JSON do IBGE Localidades
            rows.append({
                "id_municipio": int(item["id"]),
                "nome_municipio": str(item["nome"]),
                "id_uf": int(item["microrregiao"]["mesorregiao"]["UF"]["id"]),
            })
        except (KeyError, TypeError): continue
    
    return pd.DataFrame(rows).drop_duplicates(subset=["id_municipio"])

# =====================================================
# TRANSFORMER (PIB SIDRA)
# =====================================================

def transform_pib_sidra(path: str) -> pd.DataFrame:
    df = df_from_records(path)
    if df.empty: return df

    # 1. Remove a primeira linha (Cabeçalho de metadados do SIDRA)
    # Se 'NC' (Nível Territorial) estiver na coluna 0, é a linha de legenda
    if "NC" in df.columns or "Nível Territorial" in df.values:
        df = df.iloc[1:].copy()

    # 2. Rename com segurança
    rename_map = {
        "D1C": "id_municipio", "D1N": "municipio_uf_label",
        "D2C": "codigo_variavel", "D2N": "nome_variavel_raw",
        "D3C": "ano", "V": "valor_raw", "MN": "unidade"
    }
    df = df.rename(columns=rename_map)

    # 3. Conversões de Tipo
    df["id_municipio"] = _safe_int(df["id_municipio"])
    df["ano"] = _safe_int(df["ano"])
    df["codigo_variavel"] = _safe_int(df.get("codigo_variavel", pd.NA)).astype("Int64")
    
    # 4. Tratamento do Valor (Vetorizado)
    df["valor"] = parse_sidra_number(df["valor_raw"])

    # 5. Nome Canônico da Variável
    # Ex: "SIDRA:521 - Valor adicionado bruto a preços correntes"
    df["nome_variavel"] = (
        "SIDRA:" + df["codigo_variavel"].astype(str) + " - " + 
        df["nome_variavel_raw"].fillna("Desconhecida").str.strip()
    )

    # 6. Split de Município e UF (Vetorizado)
    hints = split_municipio_uf_vectorized(df["municipio_uf_label"])
    df = pd.concat([df, hints], axis=1)

    # 7. Limpeza Final
    df = df.dropna(subset=["id_municipio", "ano"])
    df["id_municipio"] = df["id_municipio"].astype(int)
    df["ano"] = df["ano"].astype(int)

    cols_final = [
        "id_municipio", "ano", "nome_variavel", "unidade", 
        "valor", "codigo_variavel", "municipio_nome_hint", "uf_sigla_hint"
    ]
    return df[cols_final]
