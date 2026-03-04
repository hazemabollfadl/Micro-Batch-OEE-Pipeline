SELECT 
RECORD:event_id::varchar as event_id,
RECORD:machine_id::varchar as machine_id,
RECORD:operational_status::varchar as operational_status,
RECORD:plant_id::varchar as plant_id,
RECORD:production.defective_units::integer as defective_units,
RECORD:production.units_produced::integer as units_produced,
RECORD:sensors.power_draw_kw::number(38,2) as power_draw_kw,
RECORD:sensors.spindle_speed_rpm::number(38,2) as spindle_speed_rpm,
RECORD:sensors.temperature_celsius::number(38,2) as temperature_celsius,
RECORD:sensors.vibration_mm_s::number(38,2) as vibration_mm_s,
RECORD:timestamp::timestamp_tz as timestamp
FROM {{source('IIOT_FACTORY', 'raw_telemetry')}}