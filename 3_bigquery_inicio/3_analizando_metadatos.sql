
/* Analizando metadata del conjunto de datos*/

/* Listando tablas en un dataset*/

SELECT table_catalog,table_schema,table_name,table_type,creation_time FROM rimac.INFORMATION_SCHEMA.TABLES;

/* Listando columnas en un dataset*/

SELECT * FROM rimac.INFORMATION_SCHEMA.COLUMNS;

/* Obteniendo conteos y tamaño de las tablas */

select * from rimac.__TABLES__;

/* Obteniendo listado de queries ejecutados*/

SELECT * FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER;

SELECT creation_time,query,state,total_bytes_processed,start_time,end_time,project_id,user_email,job_id,job_type,statement_type
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER;

/* Analizando Costos
5$ por terabyte
*/

SELECT
  user_email,
  SUM(total_bytes_billed/1024/1024/1024) AS gb_procesados,
  SUM((total_bytes_billed/1024/1024/1024/1024) * 5) AS costo_en_dolares
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS
WHERE
  job_type = 'QUERY'
  AND statement_type != 'SCRIPT'
GROUP BY
  user_email
ORDER BY 
  gb_procesados DESC;

/* Trabajos de consulta por tabla*/

SELECT
  t.project_id,
  t.dataset_id,
  t.table_id,
  COUNT(*) AS num_references
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS, UNNEST(referenced_tables) AS t
GROUP BY
  t.project_id,
  t.dataset_id,
  t.table_id
ORDER BY
  num_references DESC;

/* Consultas más costosas por proyecto*/

SELECT
 job_id,
 query,
 user_email,
 total_bytes_processed/1024/1024/1024 AS gb_procesados
, total_bytes_billed/1024/1024/1024/1024 * 5 AS costo_en_dolares,
creation_time
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE EXTRACT(DATE FROM  creation_time) = current_date()
ORDER BY costo_en_dolares DESC
LIMIT 4;

/* Consultas más largas por proyecto*/

SELECT
 job_id,
 query,
 user_email,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS job_duration_seconds,
creation_time
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE EXTRACT(DATE FROM  creation_time) = current_date()
ORDER BY job_duration_seconds DESC
LIMIT 4;
