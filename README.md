# pg-ct
postgres projects

The aim of the project is to create a change tracking system for postgres, which relies on an insert, update, delete trigger, but does not require the use of table specific ones.  The "change_type" configuration table will allow you to specify a name, schema + table, operations. Optionally, you can configure a column per change type, and also optionally, a before and after value (currently using regex). When an operation is performed on a table, and it is configured, data will be logged into the change_log table. The data included in the change_log would (at a minimum) be the primary key of the table + any columns defined in the change_type.
