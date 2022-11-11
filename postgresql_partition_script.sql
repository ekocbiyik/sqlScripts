
-- make partition
CREATE OR REPLACE FUNCTION procedure_partition_by_time() RETURNS trigger AS
$BODY$
    DECLARE
      partition_date TEXT;
      partition_date_str TEXT;
      next_month_first TIMESTAMP WITH TIME ZONE;
      this_month_first TIMESTAMP WITH TIME ZONE;
      partition TEXT;
      turkey_tz timestamp without time zone;
    BEGIN
      turkey_tz := NEW.time::timestamp without time zone;
      partition_date_str := to_char(turkey_tz,'YYYY_MM');
      partition_date := to_char(turkey_tz,'YYYY_MM_DD');
      this_month_first := to_timestamp(partition_date_str || '_01 00:00:00.000', 'YYYY_MM_DD HH24:MI:SS.MS');
      partition := TG_RELNAME || '_' || partition_date_str;
      EXECUTE 'INSERT INTO ' || partition || ' SELECT(' || TG_RELNAME || ' ' || quote_literal(NEW) || ').* RETURNING row_id;';
      RETURN NEW;
    EXCEPTION WHEN undefined_table THEN
      RAISE NOTICE 'A partition has been created %',partition;
      next_month_first = to_timestamp(partition_date_str || '_01 00:00:00.000', 'YYYY_MM_DD HH24:MI:SS.MS');
      next_month_first = next_month_first + INTERVAL '1 month';
      EXECUTE 'CREATE TABLE ' || partition || ' (check (time >= ''' || this_month_first || ''' and time < ''' || next_month_first || ''' )) INHERITS (' || TG_RELNAME || ');';
      EXECUTE 'CREATE INDEX ind_' || partition || ' on ' || partition || '(time);';
      EXECUTE 'CREATE INDEX ind_' || partition || '_host_and_time on ' || partition || '(host, time);';
      EXECUTE 'INSERT INTO ' || partition || ' SELECT(' || TG_RELNAME || ' ' || quote_literal(NEW) || ').* RETURNING row_id;';
      RETURN NEW;
    END;
  $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;
COMMIT;


--delete duplicate row from only parent table
CREATE OR REPLACE FUNCTION procedure_delete_duplicate()
    RETURNS TRIGGER AS
$$
DECLARE
      table_name TEXT;
BEGIN
    table_name := TG_RELNAME;
    EXECUTE 'DELETE FROM ONLY ' || table_name || ' where row_id = ''' || NEW.row_id || ''';';
    RETURN NEW;
END
$$
LANGUAGE plpgsql VOLATILE COST 100;



CREATE TRIGGER before_insert_trigger_for_partition
    BEFORE INSERT
    ON public.table_1
    FOR EACH ROW EXECUTE PROCEDURE public.procedure_partition_by_time();
COMMIT;


CREATE TRIGGER after_insert_trigger_for_duplicate
    AFTER INSERT
    ON table_1
    FOR EACH ROW EXECUTE PROCEDURE procedure_delete_duplicate();
COMMIT;





