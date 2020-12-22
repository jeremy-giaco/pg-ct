BEGIN;

	CREATE SCHEMA IF NOT EXISTS pgct;
	
	CREATE TABLE IF NOT EXISTS pgct.change_type
	(
		change_type_id serial not null
			constraint change_type_pkey
				primary key,
		change_type_name text,
		change_type_group_name text,
		schema_name text,
		table_name text,
		column_name text,
		on_insert boolean default false,
		on_update boolean default true,
		on_delete boolean default false,
		old_value_regex text,
		new_value_regex text,
		is_active boolean default true,
		priority integer default 0,
		created_by text default SESSION_USER,
		created_date timestamp default now(),
		modified_by text,
		modified_date timestamp
	);
	
	CREATE INDEX IF NOT EXISTS ix_change_type_schema_object_key
		ON pgct.change_type (schema_name, table_name, column_name, old_value_regex, new_value_regex);
	
	CREATE TABLE IF NOT EXISTS pgct.change_log
	(
		change_log_id bigserial not null
			constraint change_log_pkey
				primary key,
		change_type_id integer references pgct.change_type(change_type_id),
		priority smallint default 0,
		operation char,
		keys jsonb,
		status_id smallint default 0,
		created_by text default SESSION_USER,
		created_date timestamp default now(),
		modified_by text,
		modified_date timestamp
	);
	
	CREATE INDEX IF NOT EXISTS ix_change_log_created_date
		ON pgct.change_log (created_date);
	
	CREATE INDEX IF NOT EXISTS ix_change_log_change_types
		ON pgct.change_log (change_type_id);
	
	CREATE INDEX IF NOT EXISTS ix_change_log_status_id_priority
		ON pgct.change_log (status_id asc, priority desc, change_log_id asc);
	
	CREATE OR REPLACE FUNCTION pgct.fn_clean_change_log(hours integer DEFAULT 48, batch_size integer DEFAULT 10000, iterations integer DEFAULT 200) returns integer
		language plpgsql
	as $$
	DECLARE
		_i int = 0;
		_rc bigint = 1;
		_min_date timestamp;
		_deleted bigint = 0;
		_to_del bigint = 0;
		_err_context text;
	BEGIN
		_min_date = (current_timestamp - interval '1 hour' * hours);
		--find oldest change_log_id that we want to keep (so we can delete everything before it)
		SELECT 	count(*)
		INTO 		_to_del
		FROM 	pgct.change_log
		WHERE 	created_date < _min_date;
	
		RAISE NOTICE '% row(s) to delete...', _to_del;
		iterations =	CASE
							WHEN CEILING(_to_del/batch_size::float) > iterations THEN iterations
							ELSE CEILING(_to_del/batch_size::float)
						END;
	
		--loop until done or 100 iterations (to let big batches actually commit)
		WHILE _to_del > 0 and _i <= iterations LOOP
			WITH cte AS(
				SELECT	cl.change_log_id
				FROM 	pgct.change_log cl
				WHERE	created_date < _min_date
				ORDER BY cl.change_log_id
				LIMIT(batch_size)
				FOR UPDATE SKIP LOCKED
				)
				DELETE
				FROM		pgct.change_log cl
							USING cte
				WHERE	cte.change_log_id = cl.change_log_id;
			--get number of rows deleted
			GET DIAGNOSTICS _rc = ROW_COUNT;
			--get total rows to delete minus what was deleted
			_to_del = _to_del - _rc;
			--increment deleted count
			_deleted = _deleted + _rc;
			--increment counter
			_i = _i + 1;
			RAISE NOTICE 'processed batch % of %: deleted: %, remaining: %', _i, iterations, _rc, _to_del;
		END LOOP;
		RAISE NOTICE 'complete!';
		RETURN	_deleted;
		EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS _err_context = PG_EXCEPTION_CONTEXT;
			RAISE INFO 'Error Name: %', SQLERRM;
			RAISE INFO 'Error State: %', SQLSTATE;
			RAISE INFO 'Error Context: %', _err_context;
			RETURN -1;
	END;
	$$;
	
	CREATE OR REPLACE FUNCTION pgct.fn_track_inserts() RETURNS TRIGGER
		LANGUAGE plpgsql AS
	$$
	DECLARE
		_err_context TEXT;
		_created_by  TEXT;
		_keys        TEXT[];
	BEGIN
	RAISE INFO '%s%s%s', TG_OP, TG_TABLE_NAME, TG_TABLE_SCHEMA;
		IF NOT EXISTS(
			SELECT 1
			FROM pgct.change_type ct
			WHERE LOWER(ct.table_name) = LOWER(TG_TABLE_NAME)
			AND LOWER(ct.schema_name) = LOWER(TG_TABLE_SCHEMA)
			AND ct.on_insert = true
			)
			OR TG_OP != 'INSERT' THEN
				RETURN NULL ;
		END IF;
	
		--store agent_id for created_by_agent_id
		_created_by = SESSION_USER;
	
		--pivot new table and add pk (identified above)
		SELECT ARRAY_AGG(a.attname)
		INTO _keys
		FROM (
			SELECT a.attname
			FROM pg_index i
				INNER JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
			WHERE i.indrelid = TG_RELID
				AND i.indisprimary = TRUE
			UNION
			SELECT 	jn.key
			FROM  	(SELECT * FROM new_table LIMIT(1)) n, LATERAL JSONB_EACH_TEXT(TO_JSONB(n.*)) jn
			WHERE 	jn.key in ('created_by', 'created_date')
			) a;
	
		--holds tid to value mapping
		CREATE TEMP TABLE temp_column_value_new (
			temp_id     BIGINT,
			column_name TEXT,
			value       TEXT
		);
		--holds the tid to key mapping
		CREATE TEMP TABLE temp_map_new (
			temp_id BIGINT,
			keys    JSONB
		);
		--holds pk, key, and value
		CREATE TEMP TABLE temp_new (
			temp_id     BIGINT,
			keys        JSONB,
			column_name TEXT,
			value       TEXT
		);
	
		--holds id to pk
		INSERT INTO temp_column_value_new(temp_id, column_name, value)
		SELECT n.temp_id, jn.key AS column_name, jn.value AS value
		FROM (SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS temp_id, * FROM new_table) n,
			LATERAL JSONB_EACH_TEXT(TO_JSONB(n.*)) AS jn
		WHERE jn.key = ANY (_keys)
			 OR jn.key IN (
				SELECT ct.column_name
				FROM pgct.change_type ct
				WHERE ct.schema_name = TG_TABLE_SCHEMA
					AND ct.table_name = TG_TABLE_NAME
					AND COALESCE(ct.column_name, '') != ''
				);
	
		--index temp_column_value
		CREATE UNIQUE INDEX ux_temp_column_value_new_temp_id ON temp_column_value_new USING BTREE (temp_id, column_name);
	
		INSERT INTO temp_map_new(temp_id, keys)
		SELECT t.temp_id, JSONB_OBJECT_AGG(t.column_name, t.value) AS keys
		FROM temp_column_value_new t
		WHERE t.column_name = ANY (_keys)
		GROUP BY t.temp_id;
	
		--index temp_map_new
		CREATE UNIQUE INDEX ux_temp_map_new_id ON temp_map_new (temp_id);
	
		INSERT INTO temp_new(temp_id, keys, column_name, value)
		SELECT tm.temp_id, tm.keys, ti.column_name, ti.value
		FROM temp_map_new tm
			INNER JOIN temp_column_value_new ti ON tm.temp_id = ti.temp_id;
	
		CREATE INDEX cx_temp_new_temp_id ON temp_new (temp_id);
	
		INSERT INTO pgct.change_log(change_type_id, priority, operation, keys, status_id, created_date, created_by)
		SELECT DISTINCT ct.change_type_id, ct.priority, 'i', d.keys, 0 AS status_id, CURRENT_TIMESTAMP, _created_by
		FROM temp_new AS d
		INNER JOIN pgct.change_type ct
			ON ct.schema_name = TG_TABLE_SCHEMA AND ct.table_name = TG_TABLE_NAME
			AND (ct.column_name IS NULL OR ct.column_name = '' OR ct.column_name = d.column_name) AND ct.on_insert = TRUE
			AND (ct.new_value_regex IS NULL OR ct.new_value_regex = '' OR COALESCE(d.value, '') ~ ct.new_value_regex)
		WHERE ct.is_active = TRUE;
	
		DROP TABLE IF EXISTS temp_new;
		DROP TABLE IF EXISTS temp_map_new;
		DROP TABLE IF EXISTS temp_column_value_new;
	
		RETURN NULL;
		--if you dont handle these, then any error you have here will break all the things for everyone
	EXCEPTION
		WHEN OTHERS THEN GET STACKED DIAGNOSTICS _err_context = PG_EXCEPTION_CONTEXT;
		RAISE INFO 'Error Name: %', SQLERRM;
		RAISE INFO 'Error State: %', SQLSTATE;
		RAISE INFO 'Error Context: %', _err_context;
		RETURN NULL;
	END
	$$;
	
	CREATE OR REPLACE FUNCTION pgct.fn_track_updates() RETURNS TRIGGER
		LANGUAGE plpgsql AS
	$$
	DECLARE
		_err_context TEXT;
		_created_by  TEXT;
		_keys        TEXT[];
	BEGIN
		IF NOT EXISTS(
			SELECT 1
			FROM pgct.change_type ct
			WHERE LOWER(ct.table_name) = LOWER(TG_TABLE_NAME)
			AND LOWER(ct.schema_name) = LOWER(TG_TABLE_SCHEMA)
			AND ct.on_update = true
			)
			OR TG_OP != 'UPDATE' THEN
				RETURN NULL ;
		END IF;
	
		--store agent_id for created_by_agent_id
		_created_by = SESSION_USER;
	
		--pivot new table and add pk (identified above)
		SELECT ARRAY_AGG(a.attname)
		INTO _keys
		FROM (
			SELECT a.attname
			FROM pg_index i
			INNER JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
			WHERE i.indrelid = TG_RELID
				AND i.indisprimary = TRUE
			UNION
			SELECT 	jn.key
			FROM  	(SELECT * FROM new_table LIMIT(1)) n, LATERAL JSONB_EACH_TEXT(TO_JSONB(n.*)) jn
			WHERE 	jn.key in ('modified_by', 'modified_date')
		) a;
	
		--holds tid to value mapping
		CREATE TEMP TABLE temp_column_value_new (
			temp_id     BIGINT,
			column_name TEXT,
			value       TEXT
		);
		--holds the tid to key mapping
		CREATE TEMP TABLE temp_map_new (
			temp_id BIGINT,
			keys    JSONB
		);
		--holds pk, key, and value
		CREATE TEMP TABLE temp_new (
			temp_id     BIGINT,
			keys        JSONB,
			column_name TEXT,
			value       TEXT
		);
	
		--holds id to pk
		INSERT INTO temp_column_value_new(temp_id, column_name, value)
		SELECT n.temp_id, jn.key AS column_name, jn.value AS value
		FROM (SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS temp_id, * FROM new_table) n,
			LATERAL JSONB_EACH_TEXT(TO_JSONB(n.*)) AS jn
		;
	
		--index temp_column_value
		CREATE UNIQUE INDEX ux_temp_column_value_new_temp_id ON temp_column_value_new USING BTREE (temp_id, column_name);
	
		INSERT INTO temp_map_new(temp_id, keys)
		SELECT t.temp_id, JSONB_OBJECT_AGG(t.column_name, t.value) AS keys
		FROM temp_column_value_new t
		WHERE t.column_name = ANY (_keys)
		GROUP BY t.temp_id;
	
		--index temp_map_new
		CREATE UNIQUE INDEX ux_temp_map_new_id ON temp_map_new (temp_id);
	
		INSERT INTO temp_new(temp_id, keys, column_name, value)
		SELECT tm.temp_id, tm.keys, ti.column_name, ti.value
		FROM temp_map_new tm
			INNER JOIN temp_column_value_new ti ON tm.temp_id = ti.temp_id;
	
		CREATE INDEX cx_temp_new_temp_id ON temp_new (temp_id);
	
		--holds tid to value mapping
		CREATE TEMP TABLE temp_column_value_old (
			temp_id     BIGINT,
			column_name TEXT,
			value       TEXT
		);
		--holds the tid to key mapping
		CREATE TEMP TABLE temp_map_old (
			temp_id BIGINT,
			keys    JSONB
		);
		--holds pk, key, and value
		CREATE TEMP TABLE temp_old (
			temp_id     BIGINT,
			keys        JSONB,
			column_name TEXT,
			value       TEXT
		);
	
		--holds id to pk
		INSERT INTO temp_column_value_old(temp_id, column_name, value)
		SELECT n.temp_id, jn.key AS column_name, jn.value AS value
		FROM (SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS temp_id, * FROM old_table) n,
			LATERAL JSONB_EACH_TEXT(TO_JSONB(n.*)) AS jn;
	
		--index temp_column_value
		CREATE UNIQUE INDEX ux_temp_column_value_old_id ON temp_column_value_old USING BTREE (temp_id, column_name);
	
		INSERT INTO temp_map_old(temp_id, keys)
		SELECT t.temp_id, JSONB_OBJECT_AGG(t.column_name, t.value) AS keys
		FROM temp_column_value_old t
		WHERE t.column_name = ANY (_keys)
		GROUP BY t.temp_id;
		--index temp_map_old
		CREATE UNIQUE INDEX ux_temp_map_old_id ON temp_map_old (temp_id);
	
		INSERT INTO temp_old(temp_id, keys, column_name, value)
		SELECT tm.temp_id, tm.keys, ti.column_name, ti.value
		FROM temp_map_old tm
			INNER JOIN temp_column_value_old ti ON tm.temp_id = ti.temp_id;
	
		CREATE INDEX cx_temp_old_temp_id ON temp_old (temp_id);
		WITH cte_diff AS (
			SELECT
				o.keys ||
					JSONB_BUILD_OBJECT(
						'old_' || o.column_name, o.value,
						'new_' || n.column_name, n.value
					) AS keys,
				o.column_name,
				o.value AS old_value,
				n.value AS new_value
			FROM temp_new n
			INNER JOIN temp_old o
				ON n.temp_id = o.temp_id
				AND n.column_name = o.column_name
				AND n.value IS DISTINCT FROM o.value
			)
		INSERT INTO pgct.change_log(
			change_type_id,
			priority,
			operation,
			keys,
			status_id,
			created_date,
			created_by)
		SELECT DISTINCT ct.change_type_id, ct.priority, 'u', d.keys, 0 AS status_id, CURRENT_TIMESTAMP, _created_by
		FROM cte_diff AS d
		INNER JOIN pgct.change_type ct
			ON ct.schema_name = TG_TABLE_SCHEMA
				AND ct.table_name = TG_TABLE_NAME
				AND ct.on_update = TRUE
				AND (ct.column_name IS NULL OR ct.column_name = '' OR ct.column_name = d.column_name)
				AND (ct.old_value_regex IS NULL OR ct.old_value_regex = '' OR COALESCE(d.old_value, '') ~ ct.old_value_regex)
				AND (ct.new_value_regex IS NULL OR ct.new_value_regex = '' OR COALESCE(d.new_value, '') ~ ct.new_value_regex)
		WHERE ct.is_active = TRUE;
	
		DROP TABLE IF EXISTS temp_new;
		DROP TABLE IF EXISTS temp_map_new;
		DROP TABLE IF EXISTS temp_column_value_new;
	
		DROP TABLE IF EXISTS temp_old;
		DROP TABLE IF EXISTS temp_map_old;
		DROP TABLE IF EXISTS temp_column_value_old;
	
		RETURN NULL;
	
		--if you dont handle these, then any error you have here will break all the things for everyone
	EXCEPTION
		WHEN OTHERS THEN GET STACKED DIAGNOSTICS _err_context = PG_EXCEPTION_CONTEXT;
		RAISE INFO 'Error Name: %', SQLERRM;
		RAISE INFO 'Error State: %', SQLSTATE;
		RAISE INFO 'Error Context: %', _err_context;
		RETURN NULL;
	END
	$$;
	
	CREATE OR REPLACE FUNCTION pgct.fn_track_deletes() RETURNS trigger
		LANGUAGE plpgsql AS
	$$
	DECLARE
		_err_context TEXT;
		_created_by  TEXT;
		_keys        TEXT[];
	BEGIN
		IF NOT EXISTS(
			SELECT 1
			FROM pgct.change_type ct
			WHERE LOWER(ct.table_name) = LOWER(TG_TABLE_NAME)
			AND LOWER(ct.schema_name) = LOWER(TG_TABLE_SCHEMA)
			AND ct.on_delete = true
			)
			OR TG_OP != 'DELETE' THEN
				RETURN NULL ;
		END IF;
	
		--store agent_id for created_by_agent_id
		_created_by = SESSION_USER;
	
		--pivot old table and add pk (identified above)
		SELECT ARRAY_AGG(a.attname)
		INTO _keys
		FROM (
			SELECT a.attname
			FROM pg_index i
			INNER JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
			WHERE i.indrelid = TG_RELID
				AND i.indisprimary = TRUE
			) a;
	
		--holds tid to value mapping
		CREATE TEMP TABLE temp_column_value_old (
			temp_id     BIGINT,
			column_name TEXT,
			value       TEXT
		);
		--holds the tid to key mapping
		CREATE TEMP TABLE temp_map_old (
			temp_id BIGINT,
			keys    JSONB
		);
		--holds pk, key, and value
		CREATE TEMP TABLE temp_old (
			temp_id     BIGINT,
			keys        JSONB,
			column_name TEXT,
			value       TEXT
		);
	
		--holds id to pk
		INSERT INTO temp_column_value_old(temp_id, column_name, value)
		SELECT n.temp_id, jn.key AS column_name, jn.value AS value
		FROM (SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS temp_id, * FROM old_table) n,
		LATERAL JSONB_EACH_TEXT(TO_JSONB(n.*)) AS jn
		WHERE jn.key = ANY (_keys)
			OR jn.key IN (
				SELECT ct.column_name
				FROM pgct.change_type ct
				WHERE ct.schema_name = TG_TABLE_SCHEMA
					AND ct.table_name = TG_TABLE_NAME
					AND COALESCE(ct.column_name, '') != ''
				);
	
		--index temp_column_value
		CREATE UNIQUE INDEX ux_temp_column_value_old_id ON temp_column_value_old USING BTREE (temp_id, column_name);
	
		INSERT INTO temp_map_old(temp_id, keys)
		SELECT t.temp_id, JSONB_OBJECT_AGG(t.column_name, t.value) AS keys
		FROM temp_column_value_old t
		WHERE t.column_name = ANY (_keys)
		GROUP BY t.temp_id;
	
		--index temp_map_old
		CREATE UNIQUE INDEX ux_temp_map_old_id ON temp_map_old (temp_id);
	
		INSERT INTO temp_old(temp_id, keys, column_name, value)
		SELECT tm.temp_id, tm.keys, ti.column_name, ti.value
		FROM temp_map_old tm
		INNER JOIN temp_column_value_old ti ON tm.temp_id = ti.temp_id;
	
		CREATE INDEX cx_temp_old_temp_id ON temp_old (temp_id);
	
		INSERT INTO pgct.change_log( change_type_id, priority, operation, keys, status_id, created_date, created_by)
		SELECT DISTINCT ct.change_type_id, ct.priority, 'd', d.keys, 0 AS status_id, CURRENT_TIMESTAMP, _created_by
		FROM temp_old AS d
		INNER JOIN pgct.change_type ct
			ON ct.schema_name = TG_TABLE_SCHEMA
				AND ct.table_name = TG_TABLE_NAME
				AND ct.on_delete = TRUE
				AND (ct.column_name IS NULL OR ct.column_name = '' OR ct.column_name = d.column_name)
				AND (ct.old_value_regex IS NULL OR ct.old_value_regex = '' OR COALESCE(d.value, '') ~ ct.old_value_regex)
		WHERE ct.is_active = TRUE;
	
		DROP TABLE IF EXISTS temp_old;
		DROP TABLE IF EXISTS temp_map_old;
		DROP TABLE IF EXISTS temp_column_value_old;
	
		RETURN NULL;
	
		--if you dont handle these, then any error you have here will break all the things for everyone
	EXCEPTION
		WHEN OTHERS THEN GET STACKED DIAGNOSTICS _err_context = PG_EXCEPTION_CONTEXT;
		RAISE INFO 'Error Name: %', SQLERRM;
		RAISE INFO 'Error State: %', SQLSTATE;
		RAISE INFO 'Error Context: %', _err_context;
		RETURN NULL;
	END
	$$;
	
	CREATE OR REPLACE VIEW pgct.v_change_log(change_log_id, schema_name, table_name, change_type_name, change_type_group_name, priority, operation, keys, status_id, created_by, created_date) as
	SELECT 	cl.change_log_id, ct.schema_name, ct.table_name, ct.change_type_name, ct.change_type_group_name, ct.priority,
				CASE cl.operation
					WHEN 'u'::BPCHAR THEN 'UPDATE'::TEXT
					WHEN 'i'::BPCHAR THEN 'INSERT'::TEXT
					WHEN 'd'::BPCHAR THEN 'DELETE'::TEXT
					ELSE NULL::TEXT
				END AS operation, cl.keys, cl.status_id, cl.created_by, cl.created_date
	FROM 	pgct.change_log cl
				LEFT JOIN pgct.change_type ct ON (ct.change_type_id = cl.change_type_id)
	;

COMMIT;
