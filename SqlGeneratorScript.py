from numpy import insert, number
import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
import psycopg2


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
                if len(words) == 1:
                    code = row["description"][:3].upper()
                elif len(words) == 2:
                    code = (words[0][0] + words[1][:2]).upper()
                else:
                    code = row["description"][:3].upper()
                description = row["description"]
                status = 1
                creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                insert_sql = f"INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES ({catalogue_type_id}, '{code}', '{description}', {status}, '{creation_date}', '{update_date}');\n"
                sql_file.write(insert_sql)
                print("Generando inserción SQL para la fila: ", index + 1)

        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")


def generateInsertCatalogueSql(
    schemaName: str,
    tableName: str,
    pathFile: str,
    outputFileName: str,
    codeCatalogue: str,
    catalogue_type_id: number,
    start_id: int,
    start_id_catalogue: int,
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
            "catalogue_id",
            "code",
            "name",
            "description",
            "status",
            "external_code_id",
            "creation_date",
            "update_date",
            "catalogue_type_id_ab360pt_catalogue_type",
            "applies_credit_score",
            "applies_feasibility",
            "is_default",
            "parent",
            "segmentation",
        ]
        externalCodeNames = [
            "orderActionType",
            "CATALOGID",
            "chargeCode",
            "creaditReason",
        ]
        descriptionNames = ["Display value", "creaditReason Description"]

        with open(outputFileName, "w") as sql_file:
            for index, row in file.iterrows():
                catalogue_id = start_id + index
                code = codeCatalogue + "-" + str(start_id_catalogue + index).zfill(3)
                name = (
                    row.get("Short Name value")
                    if "Short Name value" in row
                    else (
                        row["Display value"]
                        if "Display value" in row
                        else row.get("creaditReason Description", "")
                    )
                )
                description = (
                    row["Display value"]
                    if "Display value" in row
                    else row.get("creaditReason Description", "")
                )
                status = 1
                external_code_id = None
                for column_name in externalCodeNames:
                    if column_name in row and not pd.isnull(row[column_name]):
                        external_code_id = row[column_name]
                        break

                if external_code_id is None:
                    print(
                        f"No se encontró un valor válido para 'external_code_id' en la fila {index + 2}."
                    )
                    continue

                creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                catalogue_type_id_ab360pt_catalogue_type = catalogue_type_id
                applies_credit_score = "null"
                applies_feasibility = "null"
                is_default = "null"
                parent = "null"
                segmentation = "B2B"

                insert_sql = f"INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES ({catalogue_id}, '{code}', '{name}', '{description}', {status}, '{external_code_id}', '{creation_date}', '{update_date}', {catalogue_type_id_ab360pt_catalogue_type}, {applies_credit_score}, {applies_feasibility}, {is_default}, {parent}, '{segmentation}');\n"
                sql_file.write(insert_sql)
                print("Generando inserción SQL para la fila: ", index + 1)

        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")


def generateMarketCatalogueInserts(
    schemaName: str,
    tableName: str,
    pathFile: str,
    outputFileName: str,
    market_id: number,
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
                    (
                        row["orderActionType"]
                        if "orderActionType" in row
                        else (
                            row["CATALOGID"]
                            if "CATALOGID" in row
                            else (
                                row["chargeCode"]
                                if "chargeCode" in row
                                else (
                                    row["creaditReason"]
                                    if "creaditReason" in row
                                    else ""
                                )
                            )
                        )
                    )
                )

                cursor.execute(
                    f"SELECT catalogue_id FROM {schemaName}.ab360pt_catalogue WHERE external_code_id = %s",
                    (display_value,),
                )
                catalogue_id_ab360pt_catalogue = str(cursor.fetchone()[0])
                status = 1
                is_default = False
                generates_charge = "null"
                charge = "null"
                is_mandatory = "null"
                order = 2
                creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                applies_skip = "null"
                is_local = "null"
                display_address = "null"
                charge_id = "null"
                charge_name = "null"

                insert_sql = f"INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES ({market_catalogue_id}, {catalogue_id_ab360pt_catalogue}, {market_id_ab360pt_market}, {status}, {is_default}, {generates_charge}, {charge}, {is_mandatory}, {order}, '{creation_date}', '{update_date}', {applies_skip}, {is_local}, {display_address}, {charge_id}, {charge_name});\n"
                sql_file.write(insert_sql)
                print("Generando inserción SQL para la fila: ", index + 1)

        conn.commit()
        cursor.close()
        conn.close()

        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")


def get_excel_file():
    root = tk.Tk()
    root.withdraw()
    filePath = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
    root.destroy()
    return filePath


filePath = get_excel_file()


if filePath:
    print(
        "======================Que tipo de archivo de insercion SQL desea generar?======================"
    )
    print("======================Catalogue Type: (CT)======================")
    print("======================Catalogue: (C)======================")
    print("======================market Catalogue (MC)======================")

    typeFileSQL = input("Ingrse la eleccion:  ")

    if typeFileSQL.upper() == "CT":

        start_id_choice = input(
            "¿Desea que le id comience desde 1 (S) o desea escribir un número de inicio personalizado (N)?: "
        ).upper()

        if start_id_choice == "S":
            start_id = 1
        else:
            start_id = int(input("Ingrese el número de inicio para id: "))

        schemaName = input("Ingrese el nombre del esquema: ")
        tableName = input("Ingrese el nombre de la tabla: ")
        outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
        outputFileName += ".sql"
        generateInsertCatalogueTypeSql(
            schemaName, tableName, filePath, outputFileName, start_id
        )

    elif typeFileSQL.upper() == "C":

        start_id_choice = input(
            "¿Desea que le id comience desde 1 (S) o desea escribir un número de inicio personalizado (N)?: "
        ).upper()

        if start_id_choice == "S":
            start_id = 1
        else:
            start_id = int(input("Ingrese el número de inicio para id: "))

        start_id_choice_catalogue = input(
            "¿Desea que le id de catalogo comience desde 1 (S) o desea escribir un número de inicio personalizado (N)?: "
        ).upper()

        if start_id_choice_catalogue == "S":
            start_id_catalogue = 1
        else:
            start_id_catalogue = int(
                input("Ingrese el número de inicio para id de catálogo XXX-00(id): ")
            )

        schemaName = input("Ingrese el nombre del esquema: ")
        tableName = input("Ingrese el nombre de la tabla: ")
        outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
        outputFileName += ".sql"
        codeCatalogue = input("Ingrese el código del catálogo: ")
        catalogue_type_id = input("Ingrese el id del catálogo: ")
        generateInsertCatalogueSql(
            schemaName,
            tableName,
            filePath,
            outputFileName,
            codeCatalogue,
            catalogue_type_id,
            start_id,
            start_id_catalogue,
        )

    elif typeFileSQL.upper() == "MC":

        start_id_choice = input(
            "¿Desea que le id comience desde 1 (S) o desea escribir un número de inicio personalizado (N)?: "
        ).upper()

        if start_id_choice == "S":
            start_id = 1
        else:
            start_id = int(input("Ingrese el número de inicio para id: "))

        schemaName = input("Ingrese el nombre del esquema: ")
        tableName = input("Ingrese el nombre de la tabla: ")
        outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
        outputFileName += ".sql"
        market_id = input("Ingrese el id del mercado: ")
        db_connection_string = (
            "postgresql://amdocs:4Md0cs.2024!@localhost:5432/amdcor360dbprod"
        )
        generateMarketCatalogueInserts(
            schemaName,
            tableName,
            filePath,
            outputFileName,
            market_id,
            start_id,
            db_connection_string,
        )
