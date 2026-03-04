SELECT DISTINCT 
MD5(machine_id || '-' || plant_id) AS machine_key,
machine_id,
plant_id
FROM {{ref('stg_telemetry')}}