-- Requires MySQL 8.0.16 or later. The schema relies on enforced CHECK constraints and NO PAD collation semantics.
CREATE TABLE easy_workflow_instances (
    id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PRIMARY KEY,
    definition LONGBLOB NOT NULL,
    status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    initiator VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    current_node_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    data LONGBLOB,
    node_state LONGBLOB,
    tasks_is_nil BOOLEAN NOT NULL,
    audit_is_nil BOOLEAN NOT NULL,
    version DECIMAL(20, 0) NOT NULL,
    CONSTRAINT easy_workflow_instances_id_nonempty CHECK (id <> ''),
    CONSTRAINT easy_workflow_instances_version_range CHECK (version >= 0 AND version <= 18446744073709551615)
) ENGINE = InnoDB;

CREATE TABLE easy_workflow_tasks (
    instance_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    ordinal BIGINT NOT NULL,
    task_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    status VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    payload LONGBLOB NOT NULL,
    PRIMARY KEY (instance_id, ordinal),
    UNIQUE KEY easy_workflow_tasks_instance_task_uq (instance_id, task_id),
    CONSTRAINT easy_workflow_tasks_instance_fk FOREIGN KEY (instance_id)
        REFERENCES easy_workflow_instances (id) ON DELETE CASCADE,
    CONSTRAINT easy_workflow_tasks_ordinal_nonnegative CHECK (ordinal >= 0),
    CONSTRAINT easy_workflow_tasks_task_nonempty CHECK (task_id <> ''),
    CONSTRAINT easy_workflow_tasks_status_nonempty CHECK (status <> '')
) ENGINE = InnoDB;

CREATE TABLE easy_workflow_audit (
    instance_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    ordinal BIGINT NOT NULL,
    action VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    payload LONGBLOB NOT NULL,
    PRIMARY KEY (instance_id, ordinal),
    CONSTRAINT easy_workflow_audit_instance_fk FOREIGN KEY (instance_id)
        REFERENCES easy_workflow_instances (id) ON DELETE CASCADE,
    CONSTRAINT easy_workflow_audit_ordinal_nonnegative CHECK (ordinal >= 0),
    CONSTRAINT easy_workflow_audit_action_nonempty CHECK (action <> '')
) ENGINE = InnoDB;
