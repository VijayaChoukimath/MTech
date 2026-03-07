def check_alerts(results):
    alerts = []
    for r in results:
        if r["Filter_Time"] > 3:
            alerts.append(f"Slow filter detected: {r['Format']}")
        if r["Storage_MB"] > 20:
            alerts.append(f"Large storage detected: {r['Format']}")
    return alerts