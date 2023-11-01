CREATE TABLE source (
                        id INT,
                        ts BIGINT,
                        vc INT
) WITH (
      'connector' = 'datagen',
      'rows-per-second'='1',  -- How many rows generated per second
      'fields.id.kind'='random',
      'fields.id.min'='1', -- min value
      'fields.id.max'='10', -- max value
      'fields.ts.kind'='sequence',
      'fields.ts.start'='1',
      'fields.ts.end'='1000000', -- end until 1000000
      'fields.vc.kind'='random',
      'fields.vc.min'='1',
      'fields.vc.max'='100'
      );