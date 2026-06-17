#!/usr/bin/env python3
# trainer.py
# Fine-tuning incremental sobre el MISMO modelo y preprocessor

import os
import sys
import joblib
import numpy as np
import pandas as pd
from scipy import sparse as sp
import tensorflow as tf
from tensorflow.keras.models import load_model
from tensorflow.keras.optimizers import Adam
from keras.losses import BinaryFocalCrossentropy
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.utils import class_weight
import keras
import warnings
import absl.logging

absl.logging.set_verbosity(absl.logging.ERROR)

# =========================
# Configuración de archivos
# =========================
HISTORY_FILE = "Data_test.csv"      
PREPROC_FILE = "preprocessor.joblib"
MODEL_FILE = "Model_V1.h5"

# =========================
# Helpers
# =========================
def to_dense(X):
    if sp.issparse(X):
        X = X.toarray()
    return X.astype("float32")

# =========================
# 1) Validar archivos
# =========================
for f in [HISTORY_FILE, PREPROC_FILE, MODEL_FILE]:
    if not os.path.exists(f):
        print(f"[ERROR] No existe: {f}")
        sys.exit(1)


df = pd.read_csv(HISTORY_FILE)

if "disposition_label" not in df.columns:
    print("[ERROR] El archivo no contiene 'disposition_label'")
    sys.exit(1)

# =========================
# 2) Separar X / y
# =========================
y = np.where(df["disposition_label"].str.upper().str.startswith("TP"), 1, 0).astype("float32")
drop_cols = ["disposition_label", "pred_label", "pred_probability"]
X_df = df.drop(columns=[c for c in drop_cols if c in df.columns])

# =========================
# 2b) Limpieza de columnas de texto
# =========================
X_df = X_df.replace({np.nan: ""})
obj_cols = X_df.select_dtypes(include=["object"]).columns
X_df[obj_cols] = X_df[obj_cols].astype(str)

# =========================
# 3) Cargar preprocessor existente
# =========================

preproc = joblib.load(PREPROC_FILE)
X = preproc.transform(X_df)
X = to_dense(X)


# =========================
# 4) Cargar modelo existente y recompilar
# =========================

model = load_model(MODEL_FILE, compile=False)

# Verificar dimensiones
if X.shape[1] != model.input_shape[1]:
    print(f"[ERROR] La dimensión de X ({X.shape[1]}) no coincide con la esperada por el modelo ({model.input_shape[1]})")
    sys.exit(1)

# Recompilar
model.compile(
    optimizer=Adam(learning_rate=1e-3),
    loss=BinaryFocalCrossentropy(alpha=0.75, gamma=2.0),
    metrics=[
        'accuracy',
        tf.keras.metrics.Precision(name='precision'),
        tf.keras.metrics.Recall(name='recall'),
        tf.keras.metrics.AUC(curve='PR', name='pr_auc')
    ]
)

# =========================
# 5) Calcular class weights
# =========================
classes = np.unique(y)
cw = class_weight.compute_class_weight("balanced", classes=classes, y=y)
class_weights = dict(zip(classes, cw))

# =========================
# 6) Fine-tuning incremental
# =========================

early_stopping = EarlyStopping( monitor='val_accuracy', patience=20, mode="max", restore_best_weights=True )


model.fit(
    X,
    y,
    epochs=100,
    batch_size=256,
    shuffle=True,
    validation_split=0.1,
    class_weight=class_weights,
    callbacks = [early_stopping],
    verbose=0
)

# =========================
# 7) Guardar modelo actualizado
# =========================
keras.saving.save_model(model, MODEL_FILE)
print("\n[OK] Modelo actualizado y sobrescrito correctamente ✔")
