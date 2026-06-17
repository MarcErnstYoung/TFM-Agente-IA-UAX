#!/usr/bin/env python3
# predictor.py
# Uso: python predictor.py data_TPFP.csv
# Requisitos: preprocessor.joblib y Model_V1.h5 en la misma carpeta.

import sys
import os
import joblib
import numpy as np
import pandas as pd
import warnings
import argparse

from scipy import sparse as sp

# Keras / TensorFlow
import tensorflow as tf
from tensorflow.keras.models import load_model  # type: ignore

warnings.filterwarnings("ignore")


def load_preprocessor(path="preprocessor.joblib"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró el preprocessor en: {path}")
    return joblib.load(path)


def load_keras_model(path="Model_V1.h5"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró el modelo Keras en: {path}")
    # Cargamos sin compilar para evitar problemas de métricas/loss custom
    return load_model(path, compile=False)


def prepare_dataframe(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No se encontró el CSV: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = df.columns.str.strip()

    # Rellenar NaN
    df = df.replace({np.nan: ""})

    # Asegurar strings en columnas object
    obj_cols = df.select_dtypes(include=["object"]).columns
    df[obj_cols] = df[obj_cols].astype(str)

    # Normalización específica
    if "dest_port" in df.columns:
        df["dest_port"] = (
            df["dest_port"]
            .astype(str)
            .str.replace(r"[\[\],]", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    return df


def transform_with_preprocessor(preproc, df):
    try:
        return preproc.transform(df)
    except Exception as e:
        raise RuntimeError(f"Error al transformar con el preprocessor: {e}")


def to_model_input(X):
    if sp.issparse(X):
        X = X.toarray()
    return np.asarray(X, dtype="float32")


def main(args):
    # 1) Cargar preprocessor y modelo
    preproc = load_preprocessor(args.preprocessor)
    model = load_keras_model(args.model)

    # 2) Cargar CSV
    df = prepare_dataframe(args.csv)

    # 3) Transformar
    X_trans = transform_with_preprocessor(preproc, df)

    # 4) Preparar input del modelo
    X_in = to_model_input(X_trans)

    # 5) Inferencia (probabilidades)
    try:
        y_prob = model.predict(X_in, batch_size=256).ravel()
    except Exception:
        preds = model.predict(X_in, batch_size=256)
        y_prob = np.asarray(preds).reshape(-1)

    # 6) Imprimir SOLO pred_probability (una por línea)
    for p in y_prob:
        print(float(p))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inferencia con modelo MLP (solo imprime pred_probability)"
    )
    parser.add_argument("csv", help="Archivo CSV de entrada")
    parser.add_argument(
        "--preprocessor",
        default="preprocessor.joblib",
        help="Archivo joblib del preprocessor (default: preprocessor.joblib)",
    )
    parser.add_argument(
        "--model",
        default="Model_V1.h5",
        help="Archivo del modelo Keras (default: Model_V1.h5)",
    )

    args = parser.parse_args()

    try:
        main(args)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
