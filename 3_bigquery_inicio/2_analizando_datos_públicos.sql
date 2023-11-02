
/* Analizando tablas del conjunto de datos público 
1. Crear conjunto de datos rimac
1. Copiar `bigquery-public-data.google_trends.top_terms` -> `rimac.google_top_terms`
2. Copiar `bigquery-public-data.wikipedia.pageviews_2023` -> `rimac.wiki_pageviews_2023`
*/

/* Duplicando google_trends.top_terms a nuestro dataset*/

CREATE OR REPLACE TABLE `rimac.google_top_terms_sin_particionado` AS
select * from `bigquery-public-data.google_trends.top_terms`;

/*
Consultando una tabla no particionada: 1.27 GB
*/

-- Esta consulta muestra una lista de los términos de búsqueda más populares diarios en Google.
SELECT
   refresh_date AS dia,
   term AS busqueda_top,
       -- Estos términos de búsqueda están entre los 25 principales en EE. UU. cada día.
   rank,
FROM `rimac.google_top_terms_sin_particionado`
WHERE
   rank = 1
       -- Elegir solo el término principal de cada día.
   AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK)
       -- Filtrar a las últimas 2 semanas.
GROUP BY dia, busqueda_top, rank
ORDER BY dia DESC;
   -- Mostrar los días en orden cronológico inverso.

/*
Comparar Resultados con Tabla Particionada: 1.27 GB vs 517 MB
*/
-- Esta consulta muestra una lista de los términos de búsqueda más populares diarios en Google.
SELECT
   refresh_date AS dia,
   term AS busqueda_top,
       -- Estos términos de búsqueda están entre los 25 principales en EE. UU. cada día.
   rank,
FROM `rimac.google_top_terms`
WHERE
   rank = 1
       -- Elegir solo el término principal de cada día.
   AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK)
       -- Filtrar a las últimas 2 semanas.
GROUP BY dia, busqueda_top, rank
ORDER BY dia DESC;
   -- Mostrar los días en orden cronológico inverso.

/*
Consultando tabla de paginas vistas de Wikipedia --4.57 GB
*/

SELECT title,sum(views) FROM `rimac.wiki_pageviews_2023`
where datehour = '2023-10-31'
group by title
order by 2 desc
limit 25;


/*
Aplicando un filtro, ¿aumentará los bytes scaneados? porque? como influye los cluster?
*/

SELECT title,sum(views) FROM `rimac.wiki_pageviews_2023`
where datehour = '2023-10-31'
and wiki in ('en.m','en')
group by title
order by 2 desc
limit 25;

/*
Creando una tabla agregada de datos particionado y clusterizada.
*/

CREATE OR REPLACE TABLE `rimac.wiki_pageviews_2023_rank_por_dia`
PARTITION BY date
CLUSTER BY rank,title
AS
with views_per_date as (
  SELECT extract(DATE FROM datehour) date,title,sum(views) views FROM `bigquery-public-data.wikipedia.pageviews_2023` 
where datehour >= '2023-01-01' 
group by extract(DATE FROM datehour),title
)
SELECT date, title, views, rank
FROM (
    SELECT 
        date,
        title,
        views,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY views DESC) as rank
    FROM views_per_date
    where title not in ('Main_Page',
  'Cookie_(informatique)',
  'Special:Search',
  'メインページ',
  '-',
  'Заглавная_страница',
  'Викисловарь:Заглавная_страница',
  'Pagina_principale',
  '特別:検索',
  'Special:搜索',
  'Wikidata:Copyright',
  'File:Youtube_logo.png',
  'Especial:Buscar',
  'Spezial:Suche',
  'Spécial:Recherche'
  )
  and title not like 'Wikipedia%' and title not like 'Wikip%' and title not like 'Special:%'
) 
WHERE rank <= 25;


/* Revisando el ranking de un dia */
select * from `rimac.wiki_pageviews_2023_rank_por_dia` where date='2023-10-31' order by views desc;


/* Comparando rankings*/

WITH google_top_terms as (
SELECT
   refresh_date AS dia,
   term AS busqueda_top,
   rank,
FROM `rimac.google_top_terms`
WHERE
   rank <=10
   AND refresh_date = '2023-10-31'
GROUP BY dia, busqueda_top, rank
ORDER BY dia DESC
),
wiki_top_paginas as(
  select * from `rimac.wiki_pageviews_2023_rank_por_dia` 
  where date = '2023-10-31'
)
select 
a.rank,
a.busqueda_top,
b.title
from google_top_terms a
left join wiki_top_paginas b
on a.rank=b.rank
order by a.rank asc;
