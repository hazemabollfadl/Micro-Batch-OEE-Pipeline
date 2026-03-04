SELECT
RECORD:timestamp::timestamp_tz as timestamp,
RECORD:price_eur_per_mwh::number(38,2) as price_eur_per_mwh
FROM {{source('IIOT_FACTORY', 'raw_energy')}}
