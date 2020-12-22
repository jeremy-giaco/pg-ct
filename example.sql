--FOR TESTING
DROP TABLE IF EXISTS public.test;
CREATE TABLE IF NOT EXISTS public.test(id bigserial primary key, value text);

DROP TRIGGER IF EXISTS trg_insert on public.test;
DROP TRIGGER IF EXISTS trg_update on public.test;
DROP TRIGGER IF EXISTS trg_delete on public.test;

CREATE TRIGGER trg_insert AFTER INSERT ON public.test REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE PROCEDURE pgct.fn_track_inserts();
CREATE TRIGGER trg_update AFTER UPDATE ON public.test REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE PROCEDURE pgct.fn_track_updates();
CREATE TRIGGER trg_delete AFTER DELETE ON public.test REFERENCING OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE PROCEDURE pgct.fn_track_deletes();

INSERT INTO pgct.change_type (change_type_id, change_type_name, change_type_group_name, schema_name, table_name, column_name, on_insert, on_update, on_delete, old_value_regex, new_value_regex, is_active, priority, created_by, created_date, modified_by, modified_date) 
VALUES 
(1, 'public.test.delete', null, 'public', 'test', null, false, false, true, null, null, true, 0, SESSION_USER, NOW(), null, null),
(2, 'public.test.insert', null, 'public', 'test', null, true, false, false, null, null, true, 0, SESSION_USER, NOW(), null, null),
(3, 'public.test.update', null, 'public', 'test', 'value', false, true, false, null, null, true, 0, SESSION_USER, NOW(), null, null),
(4, 'public.test.value.update', null, 'public', 'test', 'value', false, true, false, null, null, true, 0, SESSION_USER, NOW(), null, null)
;

INSERT INTO public.test(id, value) values (1, 'test insert');
UPDATE public.test set id = 2 where id = 1;
UPDATE public.test set value = 'test update column' where id = 2;
DELETE FROM public.test WHERE id = 2;

SELECT * FROM pgct.change_type;
SELECT * FROM pgct.change_log;
SELECT * FROM pgct.v_change_log;

