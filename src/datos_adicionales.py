import pandas as pd

def extraer_datos_adicionales(df):
    """
    Extrae las columnas relevantes de un DataFrame.
    """
    # Seleccionar las columnas específicas
    columnas_relevantes = [
        "_id",
        "fecha_ingreso_patio",
        "fecha_ingreso_inventario",
        "fechaProceso",
        "fecha_finalizado_proceso",
        "kilos",
        "canastillas",
        "kilosVaciados",
        "calidad1",
        "calidad15",
        "calidad2",
        "tipoFruta",
        "rendimiento",
        "deshidratacion",
        "enf"
    ]

    # Asegurar que las columnas existan antes de seleccionarlas
    columnas_presentes = [col for col in columnas_relevantes if col in df.columns]
    df_relevantes = df[columnas_presentes].copy()

    # Convertir campos de fecha de MongoDB al formato de cadenas estándar
    for fecha_col in ["fecha_ingreso_patio", "fecha_ingreso_inventario", "fechaProceso", "fecha_finalizado_proceso"]:
        
        # Convertir el campo `_id` al formato correcto
        if "_id" in df_relevantes.columns:
            df_relevantes["_id"] = df_relevantes["_id"].apply(
                lambda x: x.get("$oid", str(x)) if isinstance(x, dict) and "$oid" in x else str(x)
            )
        
        if fecha_col in df_relevantes.columns:
            df_relevantes[fecha_col] = df_relevantes[fecha_col].apply(
                lambda x: x["$date"] if isinstance(x, dict) and "$date" in x else None
            )

    # Convertir "canastillas" a número si es posible
    if "canastillas" in df_relevantes.columns:
        df_relevantes["canastillas"] = pd.to_numeric(df_relevantes["canastillas"], errors="coerce")

    return df_relevantes
