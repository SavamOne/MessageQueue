## Домашнее задание по курсу Route256

Реализован Kafka-consumer, читающий очередь пачкой (batch-ем). Также есть отдельная реализация counsumer-а, соблюдающий подход exactly once, с использованием Redis.

В качестве тестового Kafka-producer реализован сервис, пишущий в очередь название города и случайное значение темперутуры. В качестве batch-обработчика - логгер всего набора.  

Конфигурирование сonsumer-а производится в ConfigureServices.
