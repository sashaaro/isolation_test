# Simple transaction isolation levels tests present examples of anomalies

```bash
DATABASE_URL=postgresql://root:root@localhost:5432/isolation_level go test
```

```
+----+-------+
|name|balance|
+----+-------+
|A   |10     |
|B   |20     |
+--+---------+
```
