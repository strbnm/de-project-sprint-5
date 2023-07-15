QUERY = """
INSERT INTO stg.ordersystem_{obj}(object_id, object_value, update_ts)
VALUES (%(id)s, %(val)s, %(update_ts)s)
ON CONFLICT (object_id) DO UPDATE
SET
    object_value = EXCLUDED.object_value,
    update_ts = EXCLUDED.update_ts;
"""
