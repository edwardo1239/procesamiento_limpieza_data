import pandas as pd
from procesar_calidad import procesar_datos_calidad
from procesar_descarte import procesar_datos_descarte
from datos_adicionales import extraer_datos_adicionales



def cargar_json(file_path):
    """Cargar datos desde un archivo JSON."""
    try:
        df = pd.read_json(file_path)
        # print("Datos cargados exitosamente:")
        # print(df.head())
        return df
    except Exception as e:
        print(f"Error al cargar el archivo JSON: {e}")
        return None

if __name__ == "__main__":
    # Ruta al archivo JSON original
    file_path = "data/proceso.lotes.json"

    # Cargar el DataFrame original
    df = pd.read_json(file_path, orient="records", lines=False)

    # Procesar los datos de calidad
    df_calidad_limpio = procesar_datos_calidad(df)

    # Procesar los datos de descarte 
    df_descarte = procesar_datos_descarte(df)


    # Unir los DataFrames por la columna común "_id"
    df_combinado = pd.merge(df_calidad_limpio, df_descarte, on="_id", how="inner")

    # Extraer los datos adicionales
    df_adicionales = extraer_datos_adicionales(df)

    # Unir los datos adicionales con los datos combinados
    df_final = pd.merge(df_combinado, df_adicionales, on="_id", how="left")
    df_final = df_final[~((df_final['deshidratacion'] > 5) | (df_final['deshidratacion'] < -5))]


    # Guardar el DataFrame limpio
    df_final.to_csv("data/datos_lotes_limpios.csv", index=False)

    # Exportar a Excel
    df_final.to_excel("data/datos_lotes_limpios.xlsx", index=False)
    print("Datos guardados en formato Excel: 'data/datos_lotes_limpios.xlsx'")

    # Exportar a JSON
    df_final.to_json("data/datos_lotes_limpios.json", orient="records", lines=True, force_ascii=False)
    print("Datos guardados en formato JSON: 'data/datos_lotes_limpios.json'")
