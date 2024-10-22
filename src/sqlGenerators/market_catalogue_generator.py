import pandas as pd
from datetime import datetime
import psycopg2


def generateMarketCatalogueInserts(
    schemaName: str,
    tableName: str,
    pathFile: str,
    outputFileName: str,
    market_id: int,
    start_id: int,
    db_connection_string: str,
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
            "market_catalogue_id",
            "catalogue_id_ab360pt_catalogue",
            "market_id_ab360pt_market",
            "status",
            "is_default",
            "generates_charge",
            "charge",
            "is_mandatory",
            '"order"',
            "creation_date",
            "update_date",
            "applies_skip",
            "is_local",
            "display_address",
            "charge_id",
            "charge_name",
        ]

        conn = psycopg2.connect(db_connection_string)
        cursor = conn.cursor()

        with open(outputFileName, "w") as sql_file:
            for index, row in file.iterrows():
                market_catalogue_id = start_id + index
                market_id_ab360pt_market = market_id
                display_value = str(
                    row.get(
                        "orderActionType",
                        row.get(
                            "CATALOGID",
                            row.get("chargeCode", row.get("creaditReason", "")),
                        ),
                    )
                )

                cursor.execute(
                    f"SELECT catalogue_id FROM {schemaName}.ab360pt_catalogue WHERE external_code_id = %s",
                    (display_value,),
                )
                catalogue_id_ab360pt_catalogue = str(cursor.fetchone()[0])
                status = 1

                creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                insert_sql = f"INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES ({market_catalogue_id}, {catalogue_id_ab360pt_catalogue}, {market_id_ab360pt_market}, {status}, FALSE, NULL, NULL, NULL, 2, '{creation_date}', '{update_date}', NULL, NULL, NULL, NULL, NULL);\n"
                sql_file.write(insert_sql)
                print("Generando inserción SQL para la fila. ", index +1)

        conn.commit()
        cursor.close()
        conn.close()

        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")
