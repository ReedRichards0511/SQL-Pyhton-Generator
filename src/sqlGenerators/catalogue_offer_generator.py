import pandas as pd
from datetime import datetime
import numpy as np
import math

def generateOfferInsertSQL(
    schemaName: str,
    tableName: str,
    pathFile: str,
    outputFileName: str,
    start_id: int,
    market_id: int,
):
    if pathFile == "":
        print("La ruta del archivo está vacía")
        return

    if not pathFile.endswith(".xlsx"):
        print("El archivo no es un documento Excel (.xlsx)")
        return

    try:
        file = pd.read_excel(pathFile)
        columnsToDB = [
            "offer_id",
            "market_product_id_ab360pt_offer",
            "bill_method",
            "lob",
            "mode",
            "credit_score",
            "regular_price",
            "currency_id",
            "bundle_id",
            "cat_offer_id",
            "effective_date",
            "expiration_date",
            "max_sub_num",
            "duration",
            "status",
            "sku",
            "technology",
            "family",
            "duration_unit",
            "download",
            "upload",
            "short_desc",
            "offer_type",
            "account_category",
            "pcat_id",
            "plan_selection_type",
            "primary_offer_id",
            "display_value",
            "url",
            "upload_unit",
            "dowload_unit", 
            "plan_offer_level",
            "display_price",
            "special_characteristics",
            "visible",
            "rate_override",
        ]
        with open(outputFileName, "w") as sql_file:
            for index, row in file.iterrows():
                offer_id = row["PCAT_BUNDLE_ID"]
                market_product_id_ab360pt_offer = market_id
                bill_method = row["Bill_method"]
                lob = "null"
                mode = "null"
                credit_score = "null"
                regular_price = row["Regular_RC_Rate"]
                currency_id = row["CURRENCY_CODE"]
                bundle_id = row["PCAT_BUNDLE_ID"]
                cat_offer_id = "null"
                effective_date = row["Effective_date"]
                expiration_date = row["Expiration_date"]
                max_sub_num = "null"
                duration = "null"
                status = 1
                sku = "null"
                technology = row["TECHNOLOGY"]
                family = row["Family"]
                duration_unit = "null"
                download = "null"
                upload = "null"
                short_desc = row["SHORT_DESC"]
                offer_type = row["Offer_type"]
                account_category = "null"
                pcat_id = row["PCAT_PLAN_ID"]
                plan_selection_type = "null"
                primary_offer_id = "null"
                display_value = row["DISPLAY_VALUE"]
                url = "null"
                upload_unit = "null"
                download_unit = "null"  
                plan_offer_level = row["PLAN_OFFER_LEVEL"]

                if "REGULAR_RC_RATE_WT" in row:
                    display_price = row["REGULAR_RC_RATE_WT"]
                    if display_price is None or math.isnan(display_price):
                        display_price = 'NULL'  
                else:
                    display_price = 'NULL'

                special_characteristics = "null"
                visible = row["VISIBLE"]
                rate_override = row["RATE_OVERRIDE"]

                regular_price_sql = 'NULL' if pd.isna(regular_price) else f"{regular_price}"

                insert_sql = f"""INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES (
                    {offer_id},
                    {market_product_id_ab360pt_offer},
                    '{bill_method}',
                    NULL,
                    NULL,
                    NULL,
                    {regular_price_sql},  
                    '{currency_id}',
                    '{bundle_id}',
                    NULL,
                    '{effective_date}',
                    '{expiration_date}',
                    NULL,
                    NULL,
                    '{status}',
                    NULL,
                    '{technology}',
                    '{family}',
                    NULL,
                    NULL,
                    NULL,
                    '{short_desc}',
                    '{offer_type}',
                    '{account_category}',
                    '{pcat_id}',
                    NULL,
                    NULL,
                    '{display_value}',
                    NULL,
                    NULL,
                    NULL,
                    '{plan_offer_level}',
                    {display_price},  
                    NULL,
                    '{visible}',
                    '{rate_override}');\n"""

                sql_file.write(insert_sql)
                print("Generando SQL para la fila", index + 1)

            print(
                f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
            )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")
