SET @@dataset_project_id = 'secret-footing-366022';

CREATE SCHEMA IF not EXISTS staging_zone OPTIONS(location="us-east1");

CREATE OR REPLACE TABLE `staging_zone.DimPersona` AS
SELECT 
    p.BusinessEntityID PersonaID,
coalesce(p.Title,'') || ' ' || coalesce(p.FirstName,'') || ' '||
    coalesce(p.MiddleName,'') || ' ' ||coalesce(p.LastName,'') NombreCompleto,
    p.Title Abreviatura,
    p.FirstName PrimerNombre,
    p.MiddleName SegundoNombre,
    p.LastName ApellidoPaterno,
    p.Suffix Sufijo,
    pp.PhoneNumber Telefono,
    pnt.Name AS TipoTelefono ,
    ea.EmailAddress Correo,
    p.EmailPromotion CorreoMarketing,
    adt.Name AS TipoDireccion,
    a.AddressLine1 Direcccion1,
    a.AddressLine2 Direccion2,
    a.City Ciudad,
    sp.Name as Provincia,
    a.PostalCode CodigoPostal,
    cr.Name as Pais,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.TotalPurchaseYTD._text'),"\"","") AS DECIMAL) TotalComprasYTD,
CAST(REPLACE(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.DateFirstPurchase._text'),"\"",""),"Z","") AS DATE) PrimeraFechaCompra,
CAST(REPLACE(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.BirthDate._text'),"\"",""),"Z","") AS DATE) FechaNacimiento,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.MaritalStatus._text'),"\"","") AS STRING) EstadoCivil,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.YearlyIncome._text'),"\"","") AS STRING) IngresoAnual,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.Gender._text'),"\"","") AS STRING) Genero,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.TotalChildren._text'),"\"","") AS INTEGER) TotalHijos,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.NumberChildrenAtHome._text'),"\"","") AS INTEGER) NumeroNinosEnCasa,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.Education._text'),"\"","") AS STRING) Educacion,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.Occupation._text'),"\"","") AS STRING) Profesion,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.HomeOwnerFlag._text'),"\"","") AS INTEGER) DuenoCasa,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.NumberCarsOwned._text'),"\"","") AS INTEGER) NumeroCarros,
CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(p.Demographics),'$.IndividualSurvey.CommuteDistance._text'),"\"","") AS STRING) DistanciaTrabajo
FROM  `raw_zone.Person` p
LEFT JOIN (SELECT * FROM (
        SELECT *, ROW_NUMBER() over (PARTITION BY BusinessEntityID ORDER BY ModifiedDate DESC) RN
        FROM `raw_zone.BusinessEntityAddress`) A WHERE RN=1) bea ON bea.BusinessEntityID = p.BusinessEntityID
LEFT JOIN `raw_zone.Address` a ON a.AddressID = bea.AddressID
LEFT JOIN `raw_zone.StateProvince` sp ON sp.StateProvinceID = a.StateProvinceID
LEFT JOIN `raw_zone.CountryRegion` cr ON cr.CountryRegionCode = sp.CountryRegionCode
LEFT JOIN `raw_zone.AddressType` adt ON adt.AddressTypeID = bea.AddressTypeID
LEFT OUTER JOIN `raw_zone.EmailAddress` ea ON ea.BusinessEntityID = p.BusinessEntityID
LEFT OUTER JOIN `raw_zone.PersonPhone` pp ON pp.BusinessEntityID = p.BusinessEntityID
LEFT OUTER JOIN `raw_zone.PhoneNumberType` pnt ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;


select count(1),count(distinct PersonaID) from `staging_zone.DimPersona`;

CREATE OR REPLACE TABLE `staging_zone.DimCliente` AS
select a.CustomerID ClienteID, b.*
FROM `raw_zone.Customer` a
left join `staging_zone.DimPersona` b on a.PersonID = b.PersonaID
WHERE a.PersonID IS not NULL;

select count(1),count(distinct ClienteID) from `staging_zone.DimCliente`;

CREATE OR REPLACE TABLE `staging_zone.DimVendedor` AS
select a.BusinessEntityID VendedorID, b.*
FROM `raw_zone.SalesPerson` a
left join `staging_zone.DimPersona` b on a.BusinessEntityID = b.PersonaID;

select count(1),count(distinct VendedorID) from `staging_zone.DimVendedor`;

CREATE OR REPLACE TABLE `staging_zone.DimDistribuidor` AS
SELECT
    s.BusinessEntityID DistribuidorID,
    s.Name Distribuidor,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.AnnualSales._text'),"\"","") AS INTEGER) VentasAnuales,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.AnnualRevenue._text'),"\"","") AS INTEGER) IngresosAnuales,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.BankName._text'),"\"","") AS STRING) Banco,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.BusinessType._text'),"\"","") AS STRING) TipoNegocio,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.YearOpened._text'),"\"","") AS INTEGER) AnoApertura,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.Specialty._text'),"\"","") AS STRING) Especialidad,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.SquareFeet._text'),"\"","") AS INTEGER) MetrosCuadrados,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.Brands._text'),"\"","") AS STRING) Marcas,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.Internet._text'),"\"","") AS STRING) Internet,
    CAST(REPLACE(JSON_EXTRACT(raw_zone.xml_to_json(Demographics),'$.StoreSurvey.NumberEmployees._text'),"\"","") AS INTEGER) NumeroEmpleados
FROM `raw_zone.Store` s
left join `raw_zone.Customer` a on a.StoreID=s.BusinessEntityID and a.PersonID is null;

select count(1),count(distinct DistribuidorID) from `staging_zone.DimDistribuidor`;

CREATE OR REPLACE TABLE `staging_zone.DimTerritorio` AS
SELECT TerritoryID TerritorioID,
       Name Territorio,
       CountryRegionCode CodigoPais,
       `Group` Grupo,
       SalesYTD VentasYTD,
       SalesLastYear VentasUltimoAno,
       CostYTD CostoYTD,
       CostLastYear CostoUltimoAno
FROM `raw_zone.SalesTerritory`;

select count(1),count(distinct TerritorioID) from `staging_zone.DimTerritorio`;

CREATE OR REPLACE TABLE `staging_zone.DimProducto` AS
select
j.ProductID ProductoID,
J.Name Producto,
j.ProductNumber CodigoProducto,
j.FinishedGoodsFlag FlagProductoTerminado,
j.Color,
j.StandardCost CostoEstandar,
j.ListPrice PrecioLista,
K.Name SubCategoria,
l.Name Categoria,
m.Name Modelo
from `raw_zone.Product` j
left join `raw_zone.ProductSubcategory` k on j.ProductSubcategoryID=k.ProductSubcategoryID
left join `raw_zone.ProductCategory` l on k.ProductCategoryID=l.ProductCategoryID
left join `raw_zone.ProductModel` m on j.ProductModelID=m.ProductModelID
where j.FinishedGoodsFlag = TRUE ;

select count(1),count(distinct ProductoID) from `staging_zone.DimProducto`;


CREATE OR REPLACE TABLE `staging_zone.FactVentas` 
partition by date(FechaVenta)
AS
select  A.SalesOrderID VentaID,
        A.OrderDate FechaVenta,
        A.OnlineOrderFlag FlagVentaOnline,
        A.Status Estado,
        A.CustomerID ClienteID,
        C.StoreID DistribuidorID,
        A.SalesPersonID VendedorID,
        A.TerritoryID TerritorioID,
        count(1) Items,
        sum(B.LineTotal) MontoTotal,
        array_agg(STRUCT(
        B.ProductID as ProductoID,
        B.OrderQty as Cantidad,
        B.LineTotal as Monto)) AS Detalle
FROM `raw_zone.SalesOrderHeader` A
    LEFT JOIN `raw_zone.SalesOrderDetail` B ON A.SalesOrderID=B.SalesOrderID
    LEFT JOIN `raw_zone.Customer` C ON A.CustomerID=C.CustomerID
group by a.SalesOrderID,
        a.OrderDate,
        a.OnlineOrderFlag,
        a.Status,
        a.CustomerID,
        C.StoreID,
        a.SalesPersonID,
        a.TerritoryID;

select count(1),count(distinct VentaID) from `staging_zone.FactVentas`;
