## Тестирование уровней изоляции в PostgreSQL

Проект помогает понять, как различные уровни изоляции влияют на целостность данных в конкурентной среде.
Тесты для проверки уровней изоляции транзакций в PostgreSQL с демонстрацией аномалий, таких как:

- Dirty Read (грязное чтение)

- Non-repeatable Read (неповторяемое чтение)

- Phantom Read (фантомное чтение)

### Запуск

```bash
make postgres
make test
```

## Демонстрация

[![asciicast](https://asciinema.org/a/Bghc9BJ1jK65oYWUpnkk20nJx.svg)](https://asciinema.org/a/Bghc9BJ1jK65oYWUpnkk20nJx)
