CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
(
    { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
    [ <watermark_definition> ]
    [ <table_constraint> ][ , ...n]
)
    [COMMENT table_comment]
    [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
    WITH (key1=val1, key2=val2, ...)
    [ LIKE source_table [( <like_options> )] | AS select_query ]