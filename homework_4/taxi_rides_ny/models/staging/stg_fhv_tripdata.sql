-- Staging model for FHV (For-Hire Vehicle) tripdata 2019
-- Filters out records where dispatching_base_num IS NULL

with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(sr_flag as integer) as sr_flag,
        cast(affiliated_base_number as string) as affiliated_base_number

    from source
    -- Filter out records where dispatching_base_num IS NULL
    where dispatching_base_num is not null
)

select * from renamed
