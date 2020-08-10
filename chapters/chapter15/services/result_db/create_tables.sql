CREATE TABLE taxi_rides(
    tripduration INTEGER,
    starttime TIMESTAMP,
    start_location_id INTEGER,
    stoptime TIMESTAMP,
    end_location_id INTEGER,
    airflow_execution_date TIMESTAMP
);

CREATE TABLE citi_bike_rides(
    tripduration INTEGER,
    starttime TIMESTAMP,
    start_location_id INTEGER,
    stoptime TIMESTAMP,
    end_location_id INTEGER,
    airflow_execution_date TIMESTAMP
);
