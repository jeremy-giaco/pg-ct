# pg-ct
postgres projects

The aim of the project is to create a change tracking system for postgres, which relies on an insert, update, delete trigger, but does not require the use of table specific ones.  A configuration table (change_type) will allow you to specify what schema + table and operations you care about. Additionally, you can configure a column per change type, and optionally a before and after value (currently using regex). When an operation is performed on a table, and it is configured, data will be logged into the change_type table. The data included in the change_log would (at a minimum) be the primary key of the table + any columns defined in the change_type.
