CREATE SCHEMA IF not EXISTS staging_zone;

--https://github.com/salrashid123/bq-udf-xml
--https://stackoverflow.com/questions/50402276/big-query-user-defined-function-dramatically-slows-down-the-query
drop function if exists raw_zone.xml_to_json;
CREATE FUNCTION raw_zone.xml_to_json(a STRING)
  RETURNS STRING  
  LANGUAGE js AS
"""  
      return  frmXML(a);
"""    
OPTIONS (
  library=["gs://bk_sqlserver_ahg/xml_udf.js"]
);


DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.DimPersona`;
CREATE TABLE `focus-infusion-348919.staging_zone.DimPersona` AS
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
FROM  `focus-infusion-348919.raw_zone.Person` p
LEFT JOIN (SELECT * FROM (
        SELECT *, ROW_NUMBER() over (PARTITION BY BusinessEntityID ORDER BY ModifiedDate DESC) RN
        FROM `focus-infusion-348919.raw_zone.BusinessEntityAddress`) A WHERE RN=1) bea ON bea.BusinessEntityID = p.BusinessEntityID
LEFT JOIN `focus-infusion-348919.raw_zone.Address` a ON a.AddressID = bea.AddressID
LEFT JOIN `focus-infusion-348919.raw_zone.StateProvince` sp ON sp.StateProvinceID = a.StateProvinceID
LEFT JOIN `focus-infusion-348919.raw_zone.CountryRegion` cr ON cr.CountryRegionCode = sp.CountryRegionCode
LEFT JOIN `focus-infusion-348919.raw_zone.AddressType` adt ON adt.AddressTypeID = bea.AddressTypeID
LEFT OUTER JOIN `focus-infusion-348919.raw_zone.EmailAddress` ea ON ea.BusinessEntityID = p.BusinessEntityID
LEFT OUTER JOIN `focus-infusion-348919.raw_zone.PersonPhone` pp ON pp.BusinessEntityID = p.BusinessEntityID
LEFT OUTER JOIN `focus-infusion-348919.raw_zone.PhoneNumberType` pnt ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;


select count(1),count(distinct PersonaID) from `focus-infusion-348919.staging_zone.DimPersona`;

DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.DimCliente`;
CREATE TABLE `focus-infusion-348919.staging_zone.DimCliente` AS
select a.CustomerID ClienteID, b.*
FROM `focus-infusion-348919.raw_zone.Customer` a
left join `focus-infusion-348919.staging_zone.DimPersona` b on a.PersonID = b.PersonaID
WHERE a.PersonID IS not NULL;

select count(1),count(distinct ClienteID) from `focus-infusion-348919.staging_zone.DimCliente`;

DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.DimVendedor`;
CREATE TABLE `focus-infusion-348919.staging_zone.DimVendedor` AS
select a.BusinessEntityID VendedorID, b.*
FROM `focus-infusion-348919.raw_zone.SalesPerson` a
left join `focus-infusion-348919.staging_zone.DimPersona` b on a.BusinessEntityID = b.PersonaID;

select count(1),count(distinct VendedorID) from `focus-infusion-348919.staging_zone.DimVendedor`;

DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.DimDistribuidor`;
CREATE TABLE `focus-infusion-348919.staging_zone.DimDistribuidor` AS
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
FROM `focus-infusion-348919.raw_zone.Store` s
left join `focus-infusion-348919.raw_zone.Customer` a on a.StoreID=s.BusinessEntityID and a.PersonID is null;

select count(1),count(distinct DistribuidorID) from `focus-infusion-348919.staging_zone.DimDistribuidor`;

DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.DimTerritorio`;
CREATE TABLE `focus-infusion-348919.staging_zone.DimTerritorio` AS
SELECT TerritoryID TerritorioID,
       Name Territorio,
       CountryRegionCode CodigoPais,
       `Group` Grupo,
       SalesYTD VentasYTD,
       SalesLastYear VentasUltimoAno,
       CostYTD CostoYTD,
       CostLastYear CostoUltimoAno
FROM `focus-infusion-348919.raw_zone.SalesTerritory`;

select count(1),count(distinct TerritorioID) from `focus-infusion-348919.staging_zone.DimTerritorio`;

DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.DimProducto`;
CREATE TABLE `focus-infusion-348919.staging_zone.DimProducto` AS
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
from `focus-infusion-348919.raw_zone.Product` j
left join `focus-infusion-348919.raw_zone.ProductSubcategory` k on j.ProductSubcategoryID=k.ProductSubcategoryID
left join `focus-infusion-348919.raw_zone.ProductCategory` l on k.ProductCategoryID=l.ProductCategoryID
left join `focus-infusion-348919.raw_zone.ProductModel` m on j.ProductModelID=m.ProductModelID
where j.FinishedGoodsFlag = TRUE ;

select count(1),count(distinct ProductoID) from `focus-infusion-348919.staging_zone.DimProducto`;


DROP TABLE IF EXISTS `focus-infusion-348919.staging_zone.FactVentas`;
CREATE TABLE `focus-infusion-348919.staging_zone.FactVentas` 
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
FROM `focus-infusion-348919.raw_zone.SalesOrderHeader` A
    LEFT JOIN `focus-infusion-348919.raw_zone.SalesOrderDetail` B ON A.SalesOrderID=B.SalesOrderID
    LEFT JOIN `focus-infusion-348919.raw_zone.Customer` C ON A.CustomerID=C.CustomerID
group by a.SalesOrderID,
        a.OrderDate,
        a.OnlineOrderFlag,
        a.Status,
        a.CustomerID,
        C.StoreID,
        a.SalesPersonID,
        a.TerritoryID;

select count(1),count(distinct VentaID) from `focus-infusion-348919.staging_zone.FactVentas`;
