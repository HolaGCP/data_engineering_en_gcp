$folder="4_bigquery_llevando_datos/laboratorio_2/data"
bq mk --dataset --location us-east1 raw_zone
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Address $folder/Address.csv schemas/Address.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.AddressType $folder/AddressType.csv schemas/AddressType.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.BusinessEntityAddress $folder/BusinessEntityAddress.csv schemas/BusinessEntityAddress.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.CountryRegion $folder/CountryRegion.csv schemas/CountryRegion.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Customer $folder/Customer.csv schemas/Customer.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.EmailAddress $folder/EmailAddress.csv schemas/EmailAddress.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Employee $folder/Employee.csv schemas/Employee.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Person $folder/Person.csv schemas/Person.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.PersonPhone $folder/PersonPhone.csv schemas/PersonPhone.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.PhoneNumberType $folder/PhoneNumberType.csv schemas/PhoneNumberType.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Product $folder/Product.csv schemas/Product.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.ProductCategory $folder/ProductCategory.csv schemas/ProductCategory.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.ProductModel $folder/ProductModel.csv schemas/ProductModel.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.ProductSubcategory $folder/ProductSubcategory.csv schemas/ProductSubcategory.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesOrderDetail $folder/SalesOrderDetail.csv schemas/SalesOrderDetail.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesOrderHeader $folder/SalesOrderHeader.csv schemas/SalesOrderHeader.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesPerson $folder/SalesPerson.csv schemas/SalesPerson.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.SalesTerritory $folder/SalesTerritory.csv schemas/SalesTerritory.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.StateProvince $folder/StateProvince.csv schemas/StateProvince.json
bq --location=us-east1 load --source_format=CSV --replace --skip_leading_rows=1 --allow_quoted_newlines raw_zone.Store $folder/Store.csv schemas/Store.json