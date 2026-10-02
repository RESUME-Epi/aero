-- Convert the stored timestamps from `timestamp without time zone` to
-- `timestamptz`, to match the UTCDateTime columns in aero.models.
--
-- The existing rows were written with datetime.now() by a server process whose
-- timezone is UTC: no TZ is set in Dockerfiles/Dockerfile.server.dev or the
-- compose file, so the container inherits the base image's UTC. The conversion
-- therefore only attaches the offset those values already implied -- it does not
-- move any value.
--
--     !! That is the one assumption here. If this database has ever been written
--     !! by a server running in another zone, those rows are in that zone and
--     !! this will label them UTC, shifting them. Check before running:
--     !!
--     !!   SELECT max(created_at) FROM dataversion;
--     !!
--     !! against the UTC time of the last ingestion. If they agree, go ahead.
--
-- Run it in the same deploy as the model change: the old code cannot write to a
-- timestamptz column and the new code cannot write to a naive one.
--
--     psql "$DATABASE_URL" -f scripts/migrate_utc_timestamps.sql

BEGIN;

DO $$
DECLARE
    target record;
BEGIN
    FOR target IN
        SELECT *
        FROM (VALUES
            ('dataversion', 'created_at'),
            ('flow',        'last_executed'),
            ('sourcetype',  'created_at'),
            ('sourceurl',   'created_at')
        ) AS t(table_name, column_name)
    LOOP
        -- Skip anything already converted, so a re-run is a no-op rather than a
        -- second conversion through the session timezone.
        IF EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_name = target.table_name
              AND c.column_name = target.column_name
              AND c.data_type = 'timestamp without time zone'
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN %I TYPE timestamptz '
                'USING %I AT TIME ZONE ''UTC''',
                target.table_name, target.column_name, target.column_name
            );
            RAISE NOTICE 'converted %.%', target.table_name, target.column_name;
        ELSE
            RAISE NOTICE 'skipped %.% (already converted or absent)',
                target.table_name, target.column_name;
        END IF;
    END LOOP;
END $$;

-- What the four columns are now. Every row should read
-- `timestamp with time zone`.
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE (table_name, column_name) IN (
    ('dataversion', 'created_at'),
    ('flow',        'last_executed'),
    ('sourcetype',  'created_at'),
    ('sourceurl',   'created_at')
)
ORDER BY table_name;

COMMIT;
