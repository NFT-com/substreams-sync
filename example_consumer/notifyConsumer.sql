-- Create this function in PG to start sending outputs / msg payload to listener

CREATE OR REPLACE FUNCTION public.notify_new_transfer()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
  row RECORD;
BEGIN
  -- Checking the Operation Type
  IF (TG_OP = 'DELETE') THEN
    row = OLD;
  ELSE
    row = NEW;
  END IF;

  -- Forming the Output as notification
  PERFORM pg_notify('transfers', row.schema || '|' || row.block_number || '|' || row.token_id || '|' || row.contract_address || '|' || row.quantity || '|' || row.from_address || '|' || row.to_address || '|' || row.tx_hash || '|' || row.timestamp::text);

  -- Returning null because it is an after trigger.
  RETURN NULL;
END;
$function$;

-- Create the trigger
CREATE TRIGGER new_transfer_trigger AFTER INSERT ON transfers
  FOR EACH ROW EXECUTE FUNCTION notify_new_transfer();