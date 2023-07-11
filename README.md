# Simple transaction isolation levels tests present examples of anomalies

```bash
DATABASE_URL=postgresql://root:root@localhost:5432/isolation_level go test
```

```
+--+----------+--------+-----+
|id|product_id|quantity|price|
+--+----------+--------+-----+
|1 |1         |3       |10   |  = 30
|2 |2         |4       |20   |  = 80
+--+----------+--------+-----+
```
