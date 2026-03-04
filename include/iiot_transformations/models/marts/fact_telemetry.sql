SELECT 
    MD5(machine_id || '-' || plant_id) AS machine_key,
    event_id,
    operational_status,
    defective_units,
    units_produced,
    power_draw_kw,
    spindle_speed_rpm,
    TEMPERATURE_CELSIUS,
    vibration_mm_s,
    stg_telemetry.timestamp as timestamp_event,
    stg_energy.price_eur_per_mwh as price_per_mwh,
    stg_energy.timestamp as timestamp_energy_prices,
    (power_draw_kw / 1000) * price_eur_per_mwh as estimated_energy_cost_eur
FROM {{ ref('stg_telemetry') }} as stg_telemetry
LEFT JOIN {{ ref('stg_energy') }} as stg_energy
ON DATE_TRUNC('hour',stg_telemetry.timestamp) = DATE_TRUNC('hour',stg_energy.timestamp)