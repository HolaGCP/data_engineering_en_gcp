CREATE SCHEMA IF not EXISTS analytics_zone;


DROP TABLE IF EXISTS `focus-infusion-348919.analytics_zone.TablonVentas`;
CREATE TABLE `focus-infusion-348919.analytics_zone.TablonVentas`
partition by date(FechaVenta)
AS
SELECT A.VentaID,A.FechaVenta,A.FlagVentaOnline,A.Estado,A.Items,A.MontoTotal,A.Detalle,
    STRUCT(B.ClienteID,B.PersonaID,B.NombreCompleto,B.Abreviatura,B.PrimerNombre,B.SegundoNombre,
    B.ApellidoPaterno,B.Sufijo,B.Telefono,B.TipoTelefono,B.Correo,B.CorreoMarketing,B.TipoDireccion,
    B.Direcccion1,B.Direccion2,B.Ciudad,B.Provincia,B.CodigoPostal,B.Pais,B.TotalComprasYTD,
    B.PrimeraFechaCompra,B.FechaNacimiento,B.EstadoCivil,B.IngresoAnual,B.Genero,B.TotalHijos,
    B.NumeroNinosEnCasa,B.Educacion,B.Profesion,B.DuenoCasa,B.NumeroCarros,B.DistanciaTrabajo) as Cliente,
    STRUCT(C.DistribuidorID,C.Distribuidor,C.VentasAnuales,C.IngresosAnuales,C.Banco,C.TipoNegocio,
    C.AnoApertura,C.Especialidad,C.MetrosCuadrados,C.Marcas,C.Internet,C.NumeroEmpleados) as Distribuidor,
    STRUCT(D.TerritorioID,D.Territorio,D.CodigoPais,D.Grupo,D.VentasYTD,D.VentasUltimoAno,D.CostoYTD,D.CostoUltimoAno) Territorio,
    STRUCT(E.VendedorID,E.PersonaID,E.NombreCompleto,E.Abreviatura,E.PrimerNombre,
    E.SegundoNombre,E.ApellidoPaterno,E.Sufijo,E.Telefono,E.TipoTelefono,E.Correo,
    E.CorreoMarketing,E.TipoDireccion,E.Direcccion1,E.Direccion2,E.Ciudad,
    E.Provincia,E.CodigoPostal,E.Pais,E.TotalComprasYTD,E.PrimeraFechaCompra,
    E.FechaNacimiento,E.EstadoCivil,E.IngresoAnual,E.Genero,E.TotalHijos,
    E.NumeroNinosEnCasa,E.Educacion,E.Profesion,E.DuenoCasa,E.NumeroCarros,E.DistanciaTrabajo) as Vendedor
FROM (
SELECT A.VentaID,A.FechaVenta,A.FlagVentaOnline,A.Estado,A.ClienteID,A.DistribuidorID,A.VendedorID,A.TerritorioID,A.Items,A.MontoTotal,
array_agg(STRUCT(
        STRUCT(E.ProductoID,E.Producto,E.CodigoProducto,E.FlagProductoTerminado,E.Color,E.CostoEstandar,E.PrecioLista,E.SubCategoria,E.Categoria,E.Modelo) AS Producto,
        D.Cantidad,
        D.Monto)) AS Detalle
FROM `focus-infusion-348919.staging_zone.FactVentas`A, UNNEST(Detalle) as D
LEFT JOIN `focus-infusion-348919.staging_zone.DimProducto` E ON D.ProductoID=E.ProductoID
GROUP BY A.VentaID,A.FechaVenta,A.FlagVentaOnline,A.Estado,A.ClienteID,A.DistribuidorID,A.VendedorID,A.TerritorioID,A.Items,A.MontoTotal) A 
LEFT JOIN `focus-infusion-348919.staging_zone.DimCliente` B ON A.ClienteID=B.ClienteID
LEFT JOIN `focus-infusion-348919.staging_zone.DimDistribuidor` C ON A.DistribuidorID=C.DistribuidorID
LEFT JOIN `focus-infusion-348919.staging_zone.DimTerritorio` D ON A.TerritorioID=D.TerritorioID
LEFT JOIN `focus-infusion-348919.staging_zone.DimVendedor` E ON A.VendedorID=E.VendedorID;


select count(1),count(distinct VentaID) from `focus-infusion-348919.analytics_zone.TablonVentas`;