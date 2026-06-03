{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by=["event_name", "user_pseudo_id"],
    unique_key='surrogate_key'
) }}

WITH raw_data AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        platform,
        app_info.version           AS app_version,
        device.mobile_marketing_name,
        device.mobile_model_name,
        device.operating_system_version,
        device.advertising_id,
        device.language,
        geo.country,
        geo.city,
        geo.continent,
        event_params,
        user_properties,
        event_value_in_usd
    FROM {{ source('firebase_raw_bm', 'events_intraday_all') }}
    WHERE 1=1
    {% if is_incremental() %}
        AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 2 DAY))
    {% else %}
        AND _TABLE_SUFFIX BETWEEN
            FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 2 DAY))
            AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE("Asia/Ho_Chi_Minh"), INTERVAL 1 DAY))
    {% endif %}
),

flattened_params AS (
    SELECT
        * EXCEPT(event_params, user_properties),

        MD5(CONCAT(
            COALESCE(user_pseudo_id, 'null'),
            CAST(event_timestamp AS STRING),
            COALESCE(event_name, 'null')
        )) AS surrogate_key,

        (SELECT AS STRUCT
            MAX(IF(key = 'ga_session_id',        value.int_value,    NULL)) AS ga_session_id,
            MAX(IF(key = 'ga_session_number',    value.int_value,    NULL)) AS ga_session_number,
            MAX(IF(key = 'level',                value.int_value,    NULL)) AS level,
            MAX(IF(key = 'play_type',            value.string_value, NULL)) AS start_type,
            MAX(IF(key = 'play_index',           value.int_value,    NULL)) AS play_index,
            SAFE_CAST(ROUND(
                MAX(IF(key = 'play_time',        value.double_value, NULL))
            ) AS INT64)                                                      AS play_duration,
            MAX(IF(key = 'lose_by',              value.string_value, NULL)) AS lose_by,
            MAX(IF(key = 'result',               value.string_value, NULL)) AS result_by,
            MAX(IF(key = 'ad_format',            value.string_value, NULL)) AS ad_format,
            MAX(IF(key = 'ad_network',           value.string_value, NULL)) AS ad_network,
            MAX(IF(key = 'network_name',         value.string_value, NULL)) AS ad_platform,
            MAX(IF(key = 'ad_duration',          value.int_value,    NULL)) AS ad_duration,
            MAX(IF(key = 'value',                value.double_value, NULL)) AS value,
            MAX(IF(key = 'currency',             value.string_value, NULL)) AS currency,
            MAX(IF(key = 'placement',            value.string_value, NULL)) AS placement,
            MAX(IF(key = 'current_screen',       value.string_value, NULL)) AS current_screen,
            MAX(IF(key = 'feature_placement',    value.string_value, NULL)) AS feature_placement,
            MAX(IF(key = 'feature_name',         value.string_value, NULL)) AS feature_name,
            MAX(IF(key = 'feature_duration',     value.int_value,    NULL)) AS feature_duration,
            MAX(IF(key = 'button_name',          value.string_value, NULL)) AS button_name,
            MAX(IF(key = 'button_placement',     value.string_value, NULL)) AS button_placement,
            MAX(IF(key = 'ad_type',              value.string_value, NULL)) AS ad_type,
            MAX(IF(key = 'ad_unit_name',         value.string_value, NULL)) AS ad_unit_name,
            MAX(IF(key = 'reason',               value.string_value, NULL)) AS reason,
            MAX(IF(key = 'resource_type',        value.string_value, NULL)) AS resource_type,
            MAX(IF(key = 'engagement_time_msec', value.int_value,    NULL)) AS engagement_time_msec,
            MAX(IF(key = 'resource_amount',      value.int_value,    NULL)) AS resource_amount,
            STRING_AGG(
                IF(key LIKE 'action_seq_%', value.string_value, NULL),
                '|' ORDER BY key
            ) AS action_sequence
        FROM UNNEST(event_params)) AS ep,

        (SELECT AS STRUCT
            MAX(IF(key = 'current_level',
                SAFE_CAST(value.string_value AS INT64), NULL))               AS current_level,
            MAX(IF(key = 'first_open_time',
                DATE(TIMESTAMP_MILLIS(value.int_value), "Asia/Ho_Chi_Minh"), NULL)) AS first_open_date,
            MAX(IF(key = 'campaign',         value.string_value, NULL))     AS campaign,
            MAX(IF(key = 'adset',            value.string_value, NULL))     AS adset,
            MAX(IF(key LIKE 'firebase_exp_%', value.string_value, NULL))    AS firebase_exp_raw
        FROM UNNEST(user_properties)) AS up,

        ARRAY(
            SELECT AS STRUCT
                key,
                value.string_value,
                value.int_value,
                value.float_value,
                value.double_value
            FROM UNNEST(event_params)
        ) AS event_params_json

    FROM raw_data
)

SELECT
    surrogate_key,
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    platform,
    app_version,
    mobile_marketing_name        AS device_mobile_marketing_name,
    mobile_model_name            AS device_mobile_model_name,
    operating_system_version     AS device_operating_system_version,
    advertising_id,
    language,
    country,
    city,
    continent,
    up.current_level,
    up.first_open_date,
    up.campaign,
    up.adset,
    CASE
        WHEN LOWER(up.firebase_exp_raw) IN ('1','true','t')  THEN 'B'
        WHEN LOWER(up.firebase_exp_raw) IN ('0','false','f') THEN 'A'
        WHEN LOWER(up.firebase_exp_raw) IN ('2')             THEN 'C'
        WHEN LOWER(up.firebase_exp_raw) IN ('3')             THEN 'D'
        ELSE up.firebase_exp_raw
    END AS firebase_exp,
    ep.ga_session_id,
    ep.ga_session_number,
    ep.level,
    ep.start_type,
    ep.play_index,
    ep.play_duration,
    ep.lose_by,
    ep.action_sequence,
    ep.result_by,
    ep.ad_format,
    ep.ad_network,
    ep.ad_platform,
    ep.ad_duration,
    ep.value,
    ep.currency,
    event_value_in_usd,
    ep.placement,
    ep.current_screen,
    ep.feature_placement,
    ep.feature_name,
    ep.feature_duration,
    ep.button_name,
    ep.button_placement,
    ep.ad_type,
    ep.ad_unit_name,
    ep.reason,
    ep.resource_type,
    ep.engagement_time_msec,
    ep.resource_amount,
    event_params_json

FROM flattened_params

{% if is_incremental() %}
    WHERE surrogate_key NOT IN (SELECT surrogate_key FROM {{ this }})
{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY surrogate_key
    ORDER BY event_timestamp
) = 1
