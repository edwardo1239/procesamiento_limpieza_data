import pandas as pd

def verificar_valores_iguales(fila, columnas=["acidez", "brix", "ratio", "peso", "zumo"]):
    """
    Verifica si más de dos valores en las columnas especificadas son iguales.
    """
    valores = fila[columnas].tolist()
    return len(valores) - len(set(valores)) > 2  # Más de dos valores repetidos

def procesar_datos_calidad(df):
    """
    Procesa los datos de calidad a partir del DataFrame original.
    Retorna un DataFrame limpio y plano.
    """
    calidad_modificada = []

    if "calidad" in df.columns:
        for index, calidad_obj in df["calidad"].items():
            original_id = df["_id"].iloc[index] 

            if isinstance(calidad_obj, dict):
                # Eliminar la clave "fotosCalidad" si existe
                calidad_obj.pop("fotosCalidad", None)  

                # Verificar y eliminar "fecha" dentro de "calidadInterna"
                if "calidadInterna" in calidad_obj and isinstance(calidad_obj["calidadInterna"], dict):
                    calidad_obj["calidadInterna"].pop("fecha", None)

                # Verificar y eliminar "fecha" dentro de "clasificacionCalidad"
                if "clasificacionCalidad" in calidad_obj and isinstance(calidad_obj["clasificacionCalidad"], dict):
                    calidad_obj["clasificacionCalidad"].pop("fecha", None)

                # Verificar y eliminar "fecha" dentro de "inspeccionIngreso"
                if "inspeccionIngreso" in calidad_obj and isinstance(calidad_obj["inspeccionIngreso"], dict):
                    calidad_obj["inspeccionIngreso"].pop("fecha", None)

                # Agregar el `_id` al objeto modificado
                calidad_obj["_id"] = original_id
                
                # Agregar el objeto modificado a la lista
                calidad_modificada.append(calidad_obj)

            else:
                # Si no es un diccionario, agregar el valor sin modificar
                calidad_modificada.append(calidad_obj)

    # Filtrar elementos no válidos (NaN)
    calidad_modificada = [item for item in calidad_modificada if pd.notna(item)]

    # Crear una lista para los datos planos
    datos_planos = []

    # Iterar sobre los datos modificados
    for item in calidad_modificada:
        registro_plano = {}

        # Aplanar "calidadInterna" sin prefijo
        if "calidadInterna" in item and isinstance(item["calidadInterna"], dict):
            registro_plano.update(item["calidadInterna"])

        # Aplanar "clasificacionCalidad" con prefijo "clas."
        if "clasificacionCalidad" in item and isinstance(item["clasificacionCalidad"], dict):
            suma_clas = 0  # Suma de los valores de clasificacionCalidad
            for key, value in item["clasificacionCalidad"].items():
                # Convertir valores mayores a 1 en porcentaje decimal
                if isinstance(value, (int, float)) and value > 1:
                    value = value / 100
                suma_clas += value  # Sumar el valor
                registro_plano[f"clas.{key}"] = value

            # Agregar la suma total de clasificacionCalidad
            registro_plano["clas.suma"] = suma_clas

        # Aplanar "inspeccionIngreso" con prefijo "insp."
        if "inspeccionIngreso" in item and isinstance(item["inspeccionIngreso"], dict):
            for key, value in item["inspeccionIngreso"].items():
                registro_plano[f"insp.{key}"] = value

        # Agregar el `_id` al registro plano
        if "_id" in item and isinstance(item["_id"], dict):
            registro_plano["_id"] = item["_id"].get("$oid", None)

        # Agregar el registro plano a la lista
        datos_planos.append(registro_plano)

    # Crear un DataFrame plano
    df_planos = pd.DataFrame(datos_planos)

    # Especificar las columnas a evaluar
    columnas = ["acidez", "brix", "ratio", "peso", "zumo"]

    # Eliminar filas con NaN o 0 en las columnas especificadas
    df_limpio = df_planos.dropna(subset=columnas)
    df_limpio = df_limpio[(df_limpio[columnas] != 0).all(axis=1)]
    df_limpio = df_limpio[~df_limpio.apply(verificar_valores_iguales, axis=1)]

        # Filtrar filas según la suma de clasificacionCalidad
    if "clas.suma" in df_limpio.columns:
        df_limpio = df_limpio[(df_limpio["clas.suma"] <= 1) & (df_limpio["clas.suma"] >= 0.9)]


    return df_limpio