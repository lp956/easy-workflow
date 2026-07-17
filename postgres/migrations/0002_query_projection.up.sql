-- Defines adapter-owned read models maintained atomically by command Store transactions.
-- Hosts own migration execution and tenant discovery; these tables do not extend the core Store interface.
CREATE TABLE easy_workflow_instance_projection (
    instance_id TEXT PRIMARY KEY REFERENCES easy_workflow_instances (id) ON DELETE CASCADE,
    definition_id TEXT NOT NULL,
    definition_version NUMERIC(20, 0) NOT NULL CHECK (definition_version >= 0 AND definition_version <= 18446744073709551615),
    instance_status TEXT NOT NULL,
    initiator TEXT NOT NULL,
    current_node_id TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    last_audit_at TIMESTAMPTZ,
    order_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX easy_workflow_instance_projection_initiator_page_idx
    ON easy_workflow_instance_projection (initiator, order_at DESC, instance_id ASC);

CREATE TABLE easy_workflow_participation_projection (
    instance_id TEXT NOT NULL REFERENCES easy_workflow_instance_projection (instance_id) ON DELETE CASCADE,
    task_ordinal BIGINT NOT NULL CHECK (task_ordinal >= 0),
    task_id TEXT NOT NULL CHECK (task_id <> ''),
    actor_id TEXT NOT NULL CHECK (actor_id <> ''),
    node_id TEXT NOT NULL CHECK (node_id <> ''),
    task_status TEXT NOT NULL CHECK (task_status <> ''),
    outcome TEXT NOT NULL,
    PRIMARY KEY (instance_id, task_id),
    UNIQUE (instance_id, task_ordinal)
);

CREATE INDEX easy_workflow_participation_projection_actor_page_idx
    ON easy_workflow_participation_projection (actor_id, task_status, instance_id, task_id);
