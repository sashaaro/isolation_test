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

```
Requirements:
Account balance record should not be greater then 20
Sum of balances should not over 40
```
