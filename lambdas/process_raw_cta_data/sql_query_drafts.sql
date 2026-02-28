-- Get actual arrival time at each station
CREATE OR REPLACE VIEW actual_arrivals AS
WITH station_transitions AS (
    SELECT 
        route_name,
        run_number,
        train_direction,
        next_stop_id AS stop_id,
        next_station_name AS station_name,
        predicted_arrival AS arrival_time,
        -- Check the station name in the following chronological record
        LEAD(next_station_name) OVER (
            PARTITION BY run_number, route_name 
            ORDER BY ingestion_timestamp ASC
        ) AS following_station
    FROM cta_output
)
SELECT 
    route_name,
    run_number,
    train_direction,
    stop_id,
    station_name,
    arrival_time
FROM station_transitions
WHERE station_name <> following_station 
   OR following_station IS NULL -- Captures the final station of the run
ORDER BY run_number, arrival_time;

-- Get number of trains per hour by line
SELECT 
    route_name,
    -- Bins the timestamp into 1-hour intervals
    time_bucket(INTERVAL '1 hour', arrival_time) AS arrival_hour,
    COUNT(DISTINCT run_number) AS distinct_trains_count
FROM actual_arrivals
GROUP BY 
    route_name, 
    arrival_hour
ORDER BY 
    arrival_hour DESC, 
    route_name;

-- Get average headway by line per hour
WITH HeadwayCalculations AS (
    SELECT 
        route_name,
        station_name,
        train_direction,
        -- Truncate to the hour for grouping
        time_bucket(INTERVAL '1 hour', arrival_time) AS arrival_hour,
        -- Calculate the gap between this train and the previous one at this specific stop
        arrival_time - LAG(arrival_time) OVER (
            PARTITION BY route_name, station_name, train_direction 
            ORDER BY arrival_time ASC
        ) AS gap
    FROM actual_arrivals
),
SecondsConversion AS (
    SELECT 
        route_name,
        arrival_hour,
        -- Convert the interval to total minutes for a readable average
        EXTRACT(epoch FROM gap) / 60.0 AS gap_minutes
    FROM HeadwayCalculations
    WHERE gap IS NOT NULL -- Remove the first train of the day per station
      AND gap < INTERVAL '1 hour' -- Optional: filter out overnight gaps if data is sparse
)
SELECT 
    route_name,
    arrival_hour,
    ROUND(AVG(gap_minutes), 2) AS avg_headway_minutes,
    ROUND(STDDEV(gap_minutes), 2) AS headway_variation,
    COUNT(*) AS observations
FROM SecondsConversion
GROUP BY 
    route_name, 
    arrival_hour
ORDER BY 
    arrival_hour DESC, 
    route_name;

-- Get total trips for a day
SELECT 
    route_name,
    CAST(arrival_time AS DATE) AS service_date,
    -- Count the unique combination of the run ID and its direction
    COUNT(DISTINCT (run_number || '-' || train_direction)) AS total_directional_trips
FROM actual_arrivals
GROUP BY 
    route_name, 
    service_date
ORDER BY 
    service_date DESC, 
    route_name;