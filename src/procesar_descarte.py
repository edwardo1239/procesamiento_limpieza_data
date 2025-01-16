import pandas as pd

def procesar_datos_descarte(df):
    descarte_array = []

    if "descarteLavado" in df.columns and "descarteEncerado" in df.columns:
        for index in df.index:
            # Obtener el `_id` correspondiente y convertirlo al formato correcto
            original_id = df["_id"].iloc[index]
            if isinstance(original_id, dict) and "$oid" in original_id:
                original_id = original_id["$oid"]

            # Crear un objeto combinado para cada fila
            registro = {"_id": original_id}

            # Procesar "descarteLavado"
            if isinstance(df.at[index, "descarteLavado"], dict):
                registro.update(df.at[index, "descarteLavado"])  # Combinar con descarteLavado

            # Procesar "descarteEncerado"
            if isinstance(df.at[index, "descarteEncerado"], dict):
                # Agregar con prefijo para diferenciar de descarteLavado
                for key, value in df.at[index, "descarteEncerado"].items():
                    registro[f"encerado.{key}"] = value

            # Agregar el registro combinado a la lista
            descarte_array.append(registro)

    # Filtrar elementos no válidos (NaN)
    descarte_array = [item for item in descarte_array if pd.notna(item)]

    # Crear un DataFrame plano
    df_planos = pd.DataFrame(descarte_array)

    return df_planos
