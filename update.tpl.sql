-- {{{ table_name }}}
IF OBJECT_ID('tempdb..#{{{ staging_table_name }}}') IS NOT NULL
DROP TABLE #{{{ staging_table_name }}}

CREATE TABLE #{{{ staging_table_name }}}(
{{ #staging_table_body }}
    {{{ . }}}
{{ /staging_table_body }}
)

INSERT INTO #{{{ staging_table_name }}}
SELECT
{{ #fields }}
    {{{ . }}}
{{ /fields }}
FROM {{{ table_name }}} AS {{ table_alias }}
{{{ joins }}}
WHERE {{{ conditions }}}

IF EXISTS(SELECT 1 FROM #{{{ staging_table_name }}})
BEGIN
    IF @dry_run = 1
    BEGIN
        PRINT 'Found matches in {{{ table_name }}}'
        SELECT
        {{ #stg_fields }}
            {{{ . }}}
        {{ /stg_fields }}
        FROM #{{{ staging_table_name }}}
    END
    ELSE
    BEGIN
        BEGIN TRANSACTION

        UPDATE tbl
        SET
        {{ #values }}
            {{{ . }}}
        {{ /values }}
        FROM {{{ table_name }}} AS tbl
        INNER JOIN #{{{ staging_table_name }}} AS stg ON {{{ stg_join_conditions }}}

        COMMIT
    END
END
DROP TABLE #{{{ staging_table_name }}}
