BEGIN;

	DROP TRIGGER IF EXISTS trg_insert on public.test;
	DROP TRIGGER IF EXISTS trg_update on public.test;
	DROP TRIGGER IF EXISTS trg_delete on public.test;

	DROP FUNCTION IF EXISTS pgct.fn_track_inserts();
	DROP FUNCTION IF EXISTS pgct.fn_track_updates();
	DROP FUNCTION IF EXISTS pgct.fn_track_deletes();
	DROP FUNCTION IF EXISTS pgct.fn_clean_change_log;

	DROP VIEW IF EXISTS pgct.v_change_log;
	DROP TABLE IF EXISTS pgct.change_log;
	DROP TABLE IF EXISTS pgct.change_type;

COMMIT;

