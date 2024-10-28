import pandas as pd
from datetime import datetime



def generateOfferDescriptionInsertSQL(
    schemaName: str,
    tableName: str,
    pathFile: str,
    outputFileName: str,
):
    if pathFile == "":
        print("La ruta del archivo esta vacia")
        return
        
    if not pathFile.endswith(".xlsx"):
        print("El archivo no es un documento Excel")
        return

    file = pd.read_excel(pathFile)
    columnsToDB  = [
        "desc_id",
        "offer_id",
        "description",
        "icon_id",
        "effective_date",
        "expiration_date"
    ]
    
    with open (outputFileName, "w") as sq_file:
        for index, row in file.iterrows():
            desc_id = index +1
            offer_id = row["PLAN_ID"]
            description = row["DESCRIPTION"]
            icon_id = row["ICON_ID"]
            effective_date = datetime.now().strftime("%Y-%m-%d")
            expiration_date = datetime.now().strftime("%Y-%m-%d")
            
            instert_sql = f"""INSERT INTO {schemaName}.{tableName} ({','.join(columnsToDB)}) VALUES (
                {desc_id},
                '{offer_id}',
                '{description}',
                '{icon_id}',
                '{effective_date}',
               '{expiration_date}'
                );\n """
            sq_file.write(instert_sql)
            print("Generando SQL para la fila", index +1)
        print(
            f"Las inserciones SQL se han generado con éxito en el archivo: {outputFileName}"
        )
    