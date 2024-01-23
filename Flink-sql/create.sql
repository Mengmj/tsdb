CREATE TABLE MyTable (
    `vin` CHAR(17),
    `timestamp` BIGINT,
    `int_filed` INTEGER,
    `double_field` DOUBLE,
    `string_filed` STRING
) WITH (
    'connector' = 'tsdb',
    'db_path' = '/data/home/mingjinmeng/playground/test_data/flink-test'
    'db_table' = 'table0'
);
INSERT INTO MyTable VALUES ("abcdefghijklmnopq", 1704038400000, 100, 1.2, "hello world");