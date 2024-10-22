import pandas as pd
from datetime import datetime


def generateInsertCatalogueTypeSql(
    schemaName: str, tableName: str, pathFile: str, outputFileName: str, start_id: int
):
    if pathFile == "":
        print("La ruta del archivo está vacía.")
        return

    if not pathFile.endswith(".xlsx"):
        print("El archivo no es un archivo Excel (.xlsx).")
        return

    try:
        file = pd.read_excel(pathFile)
        columnsToDB = [
            "catalogue_type_id",
            "code",
            "description",
            "status",
            "creation_date",
            "update_date",
        ]

        with open(outputFileName, "w") as sql_file:
            for index, row in file.iterrows():
                catalogue_type_id = start_id + index
                words = row["description"].split()
                code = (
                    (words[0][0] + words[1][:2]).upper()
                    if len(words) > 1
                    else row["description"][:3].upper()
                )
                description = row["description"]
                status = 1
                creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                insert_sql = f"INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES ({catalogue_type_id}, '{code}', '{description}', {status}, '{creation_date}', '{update_date}');\n"
                sql_file.write(insert_sql)
                print("Generando inserción SQL oara la fila", index +1)

        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")
