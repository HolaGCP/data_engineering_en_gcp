/*
Primer Query para conocer la consola de bigquery
1. Fijar los siguientes proyectos(Añadir->Destaca un proyecto por nombre): bigquery-public-data y githubarchive.
2. Revisar panel de explorador y visualizar los recursos (proyectos, conjunto de datos, tablas).
3. Destacar usa_names de bigquery-public-data.
4. Revisar tabla `usa_names.usa_1910_current` (esquema,detalle,vista_previa)
5. Entender la sintaxis del query y ejecutarlo.
6. Revisar panel de resultados 
*/

SELECT
  min(year) `año_inicio`,
  max(year) `ano_final`
FROM
  `bigquery-public-data.usa_names.usa_1910_current`;


/*
Segundo query: Analizando la cantidad de bytes scaneado y grafico de ejecucion
*/
SELECT
  name,
  SUM(number) AS total
FROM
  `bigquery-public-data.usa_names.usa_1910_current`
GROUP BY
  name
ORDER BY
  total DESC
LIMIT
  10;

/*
Tercer query: Agregando un filtro
*/
SELECT
  name,
  SUM(number) AS total
FROM
  `bigquery-public-data.usa_names.usa_1910_current`
  WHERE gender='F'
GROUP BY
  name
ORDER BY
  total DESC
LIMIT
  10;

/* Creando una tabla como resultado de una consulta*/

CREATE OR REPLACE TABLE rimac.usa_names_rank AS
SELECT
  name AS nombre,
  SUM(number) AS total
FROM
  `bigquery-public-data.usa_names.usa_1910_current`
GROUP BY
  name;

/* Creando una tabla temporal*/

-- Encuentra los 100 nombres más populares por año
CREATE TEMP TABLE top_names(year INT,name STRING, total INT, rank INT)
AS
  SELECT year,name,total,rank FROM (
 SELECT year,name,total,
 ROW_NUMBER() OVER (PARTITION BY year ORDER BY total DESC) as rank
 FROM (
  SELECT year,name,sum(number) total
  FROM `bigquery-public-data`.usa_names.usa_1910_current
  WHERE year >= 2012
  group by year,name
 ) a 
  ) WHERE rank<=100
;
-- ¿Qué nombres aparecen como palabras en las obras de Shakespeare?
select year,name,rank
FROM top_names a 
INNER JOIN (
 SELECT DISTINCT word
 FROM `bigquery-public-data`.samples.shakespeare 
) b 
on a.name=b.word
-- where year=2021
order by rank asc;
-- ¿Que porcentaje del top 100 son palabras en las obras de Shakespeare?
SELECT
 a.year as `año`,
 sum(case when b.word is null then 0 else 1 end)/count(1) porcentaje_de_uso,
 count(1) total_nombres,
 avg(case when b.word is null then null else rank end) promedio_ranking
FROM top_names a 
LEFT JOIN (
 SELECT DISTINCT word
 FROM `bigquery-public-data`.samples.shakespeare 
) b 
on a.name=b.word
group by a.year
order by 1 asc;
