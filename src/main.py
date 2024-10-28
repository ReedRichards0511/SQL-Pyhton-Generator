from utils.file_selector import getExcelFile
from sqlGenerators.catalogueType_generator import generateInsertCatalogueTypeSql
from sqlGenerators.catalogue_generator import generateInsertCatalogueSql
from sqlGenerators.market_catalogue_generator import generateMarketCatalogueInserts
from sqlGenerators.catalogue_offer_generator import generateOfferInsertSQL
from sqlGenerators.catalogue_offer_description import generateOfferDescriptionInsertSQL

def main():
    filePath = getExcelFile()

    if filePath:
        print("Seleccione el tipo de archivo SQL a generar:")
        print("1. Catalogue Type")
        print("2. Catalogue")
        print("3. Market Catalogue")
        print("4. Offer Catalogue")
        print("5. Offer Description")

        choice = input("Ingrese su elección: ").upper()

        if choice == "1":
            start_id = int(input("Ingrese el ID de inicio para Catalogue Type: "))
            schemaName = input("Ingrese el nombre del esquema: ")
            tableName = input("Ingrese el nombre de la tabla: ")
            outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
            generateInsertCatalogueTypeSql(schemaName, tableName, filePath, outputFileName + ".sql", start_id)

        elif choice == "2":
            start_id = int(input("Ingrese el ID de inicio: "))
            start_id_catalogue = int(input("Ingrese el ID de inicio para el catálogo: "))
            schemaName = input("Ingrese el nombre del esquema: ")
            tableName = input("Ingrese el nombre de la tabla: ")
            codeCatalogue = input("Ingrese el código del catálogo: ")
            catalogue_type_id = int(input("Ingrese el ID del tipo de catálogo: "))
            outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
            generateInsertCatalogueSql(
                schemaName, tableName, filePath, outputFileName + ".sql", codeCatalogue, catalogue_type_id, start_id, start_id_catalogue
            )

        elif choice == "3":
            start_id = int(input("Ingrese el ID de inicio: "))
            schemaName = input("Ingrese el nombre del esquema: ")
            tableName = input("Ingrese el nombre de la tabla: ")
            market_id = int(input("Ingrese el ID del mercado: "))
            db_connection_string = input("Ingrese la cadena de conexión de la base de datos: ")
            outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
            generateMarketCatalogueInserts(
                schemaName, tableName, filePath, outputFileName + ".sql", market_id, start_id, db_connection_string
            )
        elif choice == "4":
            start_id = int(input("Ingrese el ID de inicio para Catalogue Type: "))
            schemaName = input("Ingrese el nombre del esquema: ")
            tableName = input("Ingrese el nombre de la tabla: ")
            outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
            market_id = int(input("Ingrese el ID del mercado: "))
            generateOfferInsertSQL(schemaName, tableName, filePath, outputFileName + ".sql", start_id, market_id)
            
            
        elif choice == "5":
            schemaName = input("Ingrese el nombre del esquema: ")
            tableName = input("Ingrese el nombre de la tabla: ")
            outputFileName = input("Ingrese el nombre del archivo SQL de salida: ")
            generateOfferDescriptionInsertSQL(schemaName, tableName, filePath, outputFileName + ".sql")
            
            
if __name__ == "__main__":
    main()
