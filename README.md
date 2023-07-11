# Simple transaction isolation levels tests present examples of anomalies

```bash
DATABASE_URL=postgresql://root:root@localhost:5432/isolation_level go test
```

```
+--+----------+--------+-----+
|id|product_id|quantity|price|
+--+----------+--------+-----+
|1 |1         |10      |5    |  = 50
|2 |2         |20      |4    |  = 80
+--+----------+--------+-----+
```
