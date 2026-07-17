CREATE TABLE easy_workflow_instances (
    id TEXT PRIMARY KEY CHECK (id <> ''),
    definition BYTEA NOT NULL,
    status TEXT NOT NULL,
    initiator TEXT NOT NULL,
    current_node_id TEXT NOT NULL,
    data BYTEA,
    node_state BYTEA,
    tasks_is_nil BOOLEAN NOT NULL,
    audit_is_nil BOOLEAN NOT NULL,
    version NUMERIC(20, 0) NOT NULL CHECK (version >= 0 AND version <= 18446744073709551615)
);

CREATE TABLE easy_workflow_tasks (
    instance_id TEXT NOT NULL REFERENCES easy_workflow_instances (id) ON DELETE CASCADE,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    task_id TEXT NOT NULL CHECK (task_id <> ''),
    status TEXT NOT NULL CHECK (status <> ''),
    payload BYTEA NOT NULL,
    PRIMARY KEY (instance_id, ordinal),
    UNIQUE (instance_id, task_id)
);

CREATE TABLE easy_workflow_audit (
    instance_id TEXT NOT NULL REFERENCES easy_workflow_instances (id) ON DELETE CASCADE,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    action TEXT NOT NULL CHECK (action <> ''),
    payload BYTEA NOT NULL,
    PRIMARY KEY (instance_id, ordinal)
);
