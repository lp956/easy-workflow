-- Backfills the query projection introduced in 0002 from command-side rows created by 0001.
-- This migration is intentionally idempotent so hosts can resume an interrupted upgrade safely.
WITH definitions AS (
    SELECT
        i.id,
        i.status,
        i.initiator,
        i.current_node_id,
        convert_from(i.definition, 'UTF8')::jsonb AS definition_json
    FROM easy_workflow_instances AS i
),
audit_rows AS (
    SELECT
        a.instance_id,
        a.ordinal,
        a.action,
        convert_from(a.payload, 'UTF8')::jsonb AS payload_json
    FROM easy_workflow_audit AS a
),
audit_times AS (
    SELECT
        d.id,
        (
            SELECT (a.payload_json ->> 'at')::timestamptz
            FROM audit_rows AS a
            WHERE a.instance_id = d.id
              AND a.action = 'instance.started'
              AND (a.payload_json ->> 'at')::timestamptz <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
            ORDER BY a.ordinal
            LIMIT 1
        ) AS started_at,
        (
            SELECT (a.payload_json ->> 'at')::timestamptz
            FROM audit_rows AS a
            WHERE a.instance_id = d.id
              AND (a.payload_json ->> 'at')::timestamptz <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
            ORDER BY a.ordinal DESC
            LIMIT 1
        ) AS last_audit_at
    FROM definitions AS d
),
upserted_instances AS (
    INSERT INTO easy_workflow_instance_projection (
        instance_id, definition_id, definition_version, instance_status, initiator,
        current_node_id, started_at, last_audit_at, order_at
    )
    SELECT
        d.id,
        d.definition_json ->> 'id',
        (d.definition_json ->> 'version')::numeric,
        d.status,
        d.initiator,
        d.current_node_id,
        t.started_at,
        t.last_audit_at,
        COALESCE(t.last_audit_at, TIMESTAMPTZ 'epoch')
    FROM definitions AS d
    JOIN audit_times AS t ON t.id = d.id
    ON CONFLICT (instance_id) DO UPDATE SET
        definition_id = EXCLUDED.definition_id,
        definition_version = EXCLUDED.definition_version,
        instance_status = EXCLUDED.instance_status,
        initiator = EXCLUDED.initiator,
        current_node_id = EXCLUDED.current_node_id,
        started_at = EXCLUDED.started_at,
        last_audit_at = EXCLUDED.last_audit_at,
        order_at = EXCLUDED.order_at
    RETURNING instance_id
),
task_rows AS (
    SELECT
        t.instance_id,
        t.ordinal,
        convert_from(t.payload, 'UTF8')::jsonb AS payload_json
    FROM easy_workflow_tasks AS t
)
INSERT INTO easy_workflow_participation_projection (
    instance_id, task_ordinal, task_id, actor_id, node_id, task_status, outcome
)
SELECT
    t.instance_id,
    t.ordinal,
    t.payload_json ->> 'id',
    t.payload_json ->> 'assignee',
    t.payload_json ->> 'nodeId',
    t.payload_json ->> 'status',
    COALESCE(t.payload_json ->> 'outcome', '')
FROM task_rows AS t
JOIN upserted_instances AS u ON u.instance_id = t.instance_id
WHERE COALESCE(t.payload_json ->> 'id', '') <> ''
  AND COALESCE(t.payload_json ->> 'assignee', '') <> ''
  AND COALESCE(t.payload_json ->> 'nodeId', '') <> ''
  AND COALESCE(t.payload_json ->> 'status', '') <> ''
ON CONFLICT DO NOTHING;
