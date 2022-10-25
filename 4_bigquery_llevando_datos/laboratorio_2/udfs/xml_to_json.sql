CREATE OR REPLACE FUNCTION raw_zone.xml_to_json(a STRING)
  RETURNS STRING  
  LANGUAGE js AS
"""  
      return  frmXML(a);
"""    
OPTIONS (
  library=[ "gs://__bucket__/bigquery/udfs/xml_udf.js" ]
);