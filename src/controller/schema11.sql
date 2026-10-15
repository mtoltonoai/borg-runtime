CREATE TABLE counters(kind TEXT PRIMARY KEY, value INTEGER NOT NULL);
                INSERT INTO counters(kind,value) VALUES('controller_revision',0);
                CREATE TABLE settings(name TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO settings(name,value) VALUES('database_id',lower(hex(randomblob(16))));
                CREATE TABLE tasks(id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, session TEXT NOT NULL UNIQUE, agent TEXT NOT NULL, executor TEXT NOT NULL,
                    status TEXT NOT NULL, worker TEXT, incarnation TEXT, active INTEGER NOT NULL, deadline INTEGER NOT NULL, cleanup INTEGER NOT NULL, body TEXT NOT NULL);
                CREATE INDEX ready_tasks ON tasks(status, seq);
                CREATE INDEX active_session ON tasks(session, active);
                CREATE INDEX tasks_agent ON tasks(agent, seq);
                CREATE TABLE sessions(id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, agent TEXT NOT NULL, task_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL, memory_start_seq INTEGER NOT NULL, memory_cursor_seq INTEGER NOT NULL, memory_pending TEXT);
                CREATE INDEX sessions_agent ON sessions(agent,seq);
                CREATE INDEX active_worker ON tasks(worker, incarnation, executor, active);
                CREATE INDEX cleanup_capacity ON tasks(cleanup);
                CREATE TABLE agents(id TEXT PRIMARY KEY, definition TEXT NOT NULL);
                CREATE TABLE definitions(id TEXT NOT NULL, revision TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(id,revision));
                CREATE TABLE workers(id TEXT PRIMARY KEY, body TEXT NOT NULL);
                CREATE TABLE retired_incarnations(worker TEXT NOT NULL, incarnation TEXT NOT NULL, PRIMARY KEY(worker,incarnation));
                CREATE TABLE attempts(id TEXT PRIMARY KEY, task_id TEXT NOT NULL, body TEXT NOT NULL, thread_id TEXT, turn_id TEXT, executor_outcome TEXT,
                    model_requests INTEGER NOT NULL DEFAULT 0, input_tokens INTEGER, output_tokens INTEGER,
                    control_remaining INTEGER NOT NULL DEFAULT 0 CHECK(control_remaining BETWEEN 0 AND 3));
                CREATE TABLE commands(seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, worker TEXT NOT NULL, incarnation TEXT NOT NULL,
                    task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, generation INTEGER NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0, body TEXT NOT NULL, delivery_operation_id TEXT);
                CREATE INDEX commands_for_worker ON commands(worker, incarnation, acknowledged, seq);
                CREATE TABLE receipts(id TEXT PRIMARY KEY, request TEXT NOT NULL, outcome TEXT NOT NULL, cleanup INTEGER NOT NULL DEFAULT 0, controller_revision INTEGER NOT NULL UNIQUE CHECK(controller_revision>0));
                CREATE INDEX receipt_class ON receipts(cleanup);
                CREATE TABLE external_operations(id TEXT PRIMARY KEY, body TEXT NOT NULL, prepare TEXT NOT NULL, settlement TEXT, finalization_reserved INTEGER NOT NULL DEFAULT 0 CHECK(finalization_reserved IN (0,1)));
                CREATE TABLE history(seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, revision INTEGER NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL);
                CREATE INDEX task_history ON history(task_id, seq);
                CREATE TABLE waits(child TEXT PRIMARY KEY, parent TEXT NOT NULL, parent_attempt TEXT NOT NULL, parent_generation INTEGER NOT NULL,
                    operation_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL, outcome TEXT);
                CREATE TABLE memories(agent TEXT NOT NULL, key TEXT NOT NULL, revision INTEGER NOT NULL, body TEXT NOT NULL, operation_id TEXT NOT NULL, PRIMARY KEY(agent,key));
                CREATE TABLE releases(id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, task_id TEXT NOT NULL,
                    package_sha256 TEXT NOT NULL, body TEXT NOT NULL, UNIQUE(task_id,package_sha256));
                CREATE INDEX releases_for_task ON releases(task_id,seq);
                CREATE TABLE protected_receipts(id TEXT PRIMARY KEY, producer TEXT NOT NULL, request TEXT NOT NULL, reply TEXT NOT NULL, revision INTEGER NOT NULL UNIQUE);
                CREATE TABLE protected_actions(id TEXT PRIMARY KEY, body TEXT NOT NULL);
                PRAGMA user_version = 11;
                PRAGMA application_id = 1112494642;
