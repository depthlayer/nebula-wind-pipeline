from datetime import date

from nebula.anomalies import identify_daily_anomalies


def test_daily_peer_anomaly_is_flagged(spark):
    rows = [(date(2022, 3, 1), turbine_id, 3.0) for turbine_id in range(1, 15)]
    rows.append((date(2022, 3, 1), 15, 10.0))
    df = spark.createDataFrame(rows, ["measurement_date", "turbine_id", "avg_power_mw"])

    results = {
        row.turbine_id: row
        for row in identify_daily_anomalies(df).collect()
    }

    assert results[15].is_anomaly is True
    assert results[15].anomaly_direction == "HIGH"
    assert results[1].is_anomaly is False
