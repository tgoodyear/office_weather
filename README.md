# office_weather

Uses the rtl_433 project to pipe data from F007TH temperature probes into an Influxdb database.

## Run
```
sudo rtl_433 -s 1500000 -F json | python weather_ingest.py
```

## Dashboard
Use grafana to create dashboard for temperature and humidity.


![Grafana Dashboard](Grafana_Dashboard.png?raw=true "Grafana Dashboard")
