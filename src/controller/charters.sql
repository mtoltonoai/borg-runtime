-- Root integrates this explicit additive schema migration; no runtime CREATE.
CREATE TABLE charter_sessions(id TEXT PRIMARY KEY, revision INTEGER NOT NULL, body TEXT NOT NULL);
CREATE TABLE charter_session_versions(id TEXT NOT NULL, revision INTEGER NOT NULL, body TEXT NOT NULL, operation_id TEXT, PRIMARY KEY(id,revision));
CREATE TABLE charter_requests(id TEXT PRIMARY KEY, session_id TEXT NOT NULL, attempt_id TEXT NOT NULL, request_index INTEGER NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL, UNIQUE(attempt_id,request_index));
CREATE INDEX charter_requests_session ON charter_requests(session_id,id);
CREATE TABLE charter_replies(id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
CREATE TABLE charter_closures(request_id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE charter_compaction_results(request_id TEXT PRIMARY KEY, body TEXT NOT NULL);
