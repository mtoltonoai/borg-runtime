-- The owned upgrade removes only the one-session-per-task constraint.
-- All retained session column bytes are copied unchanged, including deliveries.
CREATE TABLE sessions24(id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, agent TEXT NOT NULL, task_id TEXT NOT NULL, body TEXT NOT NULL, memory_start_seq INTEGER NOT NULL, memory_cursor_seq INTEGER NOT NULL, memory_pending TEXT, workflow_delivery TEXT);
INSERT INTO sessions24 SELECT id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending,workflow_delivery FROM sessions;
DROP TABLE sessions;
ALTER TABLE sessions24 RENAME TO sessions;
CREATE INDEX sessions_agent ON sessions(agent,seq);
CREATE INDEX sessions_task ON sessions(task_id,seq);
CREATE TABLE session_inputs(session_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, body TEXT NOT NULL, previous_session_id TEXT, operation_id TEXT UNIQUE);
INSERT INTO session_inputs(session_id,task_id,body) SELECT session,id,json_extract(body,'$.task.input') FROM tasks;
CREATE INDEX session_inputs_task ON session_inputs(task_id,session_id);
CREATE TABLE task_assignments(operation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, from_session_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','activated','cancelled')), to_session_id TEXT UNIQUE, body TEXT NOT NULL);
CREATE UNIQUE INDEX task_assignment_pending ON task_assignments(task_id) WHERE state='pending';
CREATE UNIQUE INDEX task_assignment_activation ON task_assignments(from_session_id) WHERE state='activated';
