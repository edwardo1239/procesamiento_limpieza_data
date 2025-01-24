import pandas as pd

def extraer_datos_adicionales(df):
    """
    Extrae las columnas relevantes de un DataFrame.
    """
    # Seleccionar las columnas específicas
    columnas_relevantes = [
        "_id",
        "fecha_creacion",
        "fecha_estimada_llegada",
        "fechaIngreso",
        "fecha_ingreso_patio",
        "fecha_salida_patio",
        "fecha_ingreso_inventario",
        "fechaProceso",
        "fecha_finalizado_proceso",
        "kilos",
        "canastillas",
        "kilosVaciados",
        "calidad1",
        "calidad15",
        "calidad2",
        "directoNacional",
        "tipoFruta",
        "rendimiento",
        "deshidratacion",
        "enf"
    ]

    # Lista de las columnas relevantes con fechas
    fechas_columnas = [
        'fecha_ingreso_patio',
        'fecha_ingreso_inventario',
        'fecha_estimada_llegada',
        'fechaIngreso',
        'fecha_creacion',
        'fecha_salida_patio'
    ]

    # Lista adicional
    columnas_adicionales = ["fechaProceso", "fecha_finalizado_proceso"]

    # Combinar ambas listas
    fechas_columnas_add = fechas_columnas + columnas_adicionales

    # Asegurar que las columnas existan antes de seleccionarlas
    columnas_presentes = [col for col in columnas_relevantes if col in df.columns]
    df_relevantes = df[columnas_presentes].copy()

    # Convertir campos de fecha de MongoDB al formato de cadenas estándar
    for fecha_col in fechas_columnas_add:
        
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

    # Asegúrate de que las columnas contengan solo cadenas, fechas o valores nulos
    for col in fechas_columnas:
        df_relevantes[col] = df_relevantes[col].apply(lambda x: str(x) if isinstance(x, (str, pd.Timestamp)) else None)


    # Asegúrate de que las columnas sean de tipo datetime para trabajar con fechas
    df_relevantes[fechas_columnas] = df_relevantes[fechas_columnas].apply(pd.to_datetime, errors='coerce')

    # Crear la nueva columna 'fecha_Inicio' con la fecha más antigua
    df_relevantes['fecha_Inicio'] = df_relevantes[fechas_columnas].min(axis=1)

    # Convertir columnas adicionales a formato datetime si están presentes
    for col in columnas_adicionales:
        if col in df_relevantes.columns:
            df_relevantes[col] = pd.to_datetime(df_relevantes[col], errors='coerce')
            df_relevantes[col] = df_relevantes[col].dt.strftime('%Y-%m-%d')


    # Eliminar las columnas originales de fechas
    df_relevantes = df_relevantes.drop(columns=fechas_columnas)

    # Convertir 'fecha_Inicio' al formato de texto (por ejemplo, 'YYYY-MM-DD')
    df_relevantes['fecha_Inicio'] = df_relevantes['fecha_Inicio'].dt.strftime('%Y-%m-%d')

    # Exportar a Excel
    df_relevantes.to_excel("output.xlsx", index=False)

    return df_relevantes
