import pandas as pd
from datetime import datetime


def generateInsertCatalogueSql(
    schemaName: str,
    tableName: str,
    pathFile: str,
    outputFileName: str,
    codeCatalogue: str,
    catalogue_type_id: int,
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

        with open(outputFileName, "w") as sql_file:
            for index, row in file.iterrows():
                catalogue_id = start_id + index
                code = f"{codeCatalogue}-{str(start_id_catalogue + index).zfill(3)}"
                name = row.get(
                    "Short Name value",
                    row.get("Display value", row.get("creaditReason Description", "")),
                )
                description = (
                    row["Display value"]
                    if "Display value" in row
                    else row.get("creaditReason Description", "")
                )
                status = 1
                external_code_id = next(
                    (
                        row[col]
                        for col in [
                            "orderActionType",
                            "CATALOGID",
                            "chargeCode",
                            "creaditReason",
                        ]
                        if col in row and pd.notnull(row[col])
                    ),
                    None,
                )

                if external_code_id is None:
                    print(
                        f"No se encontró un valor válido para 'external_code_id' en la fila {index + 2}."
                    )
                    continue

                creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                catalogue_type_id_ab360pt_catalogue_type = catalogue_type_id

                insert_sql = f"INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES ({catalogue_id}, '{code}', '{name}', '{description}', {status}, '{external_code_id}', '{creation_date}', '{update_date}', {catalogue_type_id_ab360pt_catalogue_type}, NULL, NULL, NULL, NULL, 'B2B');\n"
                sql_file.write(insert_sql)
                print("Generando inserción SQL para la fila: ", index +1)

        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )

    except Exception as e:
        print(f"Se produjo un error al procesar el archivo Excel: {e}")
