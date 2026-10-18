-- Permanent identity retirement; original definitions, tasks and receipts stay intact.
CREATE TABLE agent_retirements (
    id TEXT PRIMARY KEY NOT NULL,
    definition_revision TEXT NOT NULL,
    operation_id TEXT NOT NULL UNIQUE
);
