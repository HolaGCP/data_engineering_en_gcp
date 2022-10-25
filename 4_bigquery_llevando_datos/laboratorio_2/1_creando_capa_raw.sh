folder="$(pwd)/4_bigquery_llevando_datos/laboratorio_2"
bq mk --dataset --location us-east1 raw_zone
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Address $folder/data/Address.csv $folder/schemas/Address.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.AddressType $folder/data/AddressType.csv $folder/schemas/AddressType.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.BusinessEntityAddress $folder/data/BusinessEntityAddress.csv $folder/schemas/BusinessEntityAddress.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.CountryRegion $folder/data/CountryRegion.csv $folder/schemas/CountryRegion.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Customer $folder/data/Customer.csv $folder/schemas/Customer.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.EmailAddress $folder/data/EmailAddress.csv $folder/schemas/EmailAddress.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Employee $folder/data/Employee.csv $folder/schemas/Employee.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Person $folder/data/Person.csv $folder/schemas/Person.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.PersonPhone $folder/data/PersonPhone.csv $folder/schemas/PersonPhone.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.PhoneNumberType $folder/data/PhoneNumberType.csv $folder/schemas/PhoneNumberType.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Product $folder/data/Product.csv $folder/schemas/Product.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.ProductCategory $folder/data/ProductCategory.csv $folder/schemas/ProductCategory.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.ProductModel $folder/data/ProductModel.csv $folder/schemas/ProductModel.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.ProductSubcategory $folder/data/ProductSubcategory.csv $folder/schemas/ProductSubcategory.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesOrderDetail $folder/data/SalesOrderDetail.csv $folder/schemas/SalesOrderDetail.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesOrderHeader $folder/data/SalesOrderHeader.csv $folder/schemas/SalesOrderHeader.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesPerson $folder/data/SalesPerson.csv $folder/schemas/SalesPerson.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesTerritory $folder/data/SalesTerritory.csv $folder/schemas/SalesTerritory.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.StateProvince $folder/data/StateProvince.csv $folder/schemas/StateProvince.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Store $folder/data/Store.csv $folder/schemas/Store.json

# Copiando función udf para trabajar con xml
project=$(gcloud config get project)
gsutil cp $folder/udfs/xml_udf.js gs://$project/bigquery/udfs/xml_udf.js

# Creando función en el dataset raw_zone.
# como crear funciones udf -> https://github.com/salrashid123/bq-udf-xml
# porque hace mas lento las consultas las funciones de javascript -> https://stackoverflow.com/questions/50402276/big-query-user-defined-function-dramatically-slows-down-the-query
sql=$(cat "$folder/udfs/xml_to_json.sql" | sed "s/__bucket__/${project}/g" )
bq query --use_legacy_sql=false $sql