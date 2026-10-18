-- Additive schema12 to schema13 migration, within the sole controller transaction.

CREATE TABLE workflow_questions(
 id TEXT PRIMARY KEY, revision INTEGER NOT NULL CHECK(revision>0),
 created_product_revision INTEGER NOT NULL CHECK(created_product_revision>0),
 recipient TEXT NOT NULL, status TEXT NOT NULL,
 record_kind TEXT NOT NULL, record_id TEXT NOT NULL, body TEXT NOT NULL
);
CREATE INDEX workflow_questions_queue ON workflow_questions(recipient,status,created_product_revision,id);
CREATE INDEX workflow_questions_record ON workflow_questions(record_kind,record_id,created_product_revision,id);
CREATE TABLE workflow_answers(id TEXT PRIMARY KEY,question_id TEXT NOT NULL REFERENCES workflow_questions(id),product_revision INTEGER NOT NULL CHECK(product_revision>0),body TEXT NOT NULL);
CREATE INDEX workflow_answers_question ON workflow_answers(question_id,product_revision,id);
CREATE TABLE workflow_question_actions(question_id TEXT NOT NULL REFERENCES workflow_questions(id),question_revision INTEGER NOT NULL CHECK(question_revision>0),product_revision INTEGER NOT NULL CHECK(product_revision>0),body TEXT NOT NULL,PRIMARY KEY(question_id,question_revision));

CREATE TABLE workflow_documents (
    id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK(revision>0),
    created_product_revision INTEGER NOT NULL UNIQUE CHECK(created_product_revision>0),
    body TEXT NOT NULL
);
CREATE TABLE workflow_document_versions (
    document_id TEXT NOT NULL REFERENCES workflow_documents(id),
    version INTEGER NOT NULL CHECK(version>0),
    body TEXT NOT NULL,
    PRIMARY KEY(document_id,version)
);
CREATE TABLE workflow_comments (
    id TEXT PRIMARY KEY,
    record_kind TEXT NOT NULL CHECK(record_kind IN ('task','question','document','review')),
    record_id TEXT NOT NULL,
    created_product_revision INTEGER NOT NULL UNIQUE CHECK(created_product_revision>0),
    body TEXT NOT NULL
);
CREATE INDEX workflow_comments_subject ON workflow_comments(record_kind,record_id,created_product_revision,id);
CREATE TABLE workflow_reviews (
    id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL CHECK(revision>0),
    created_product_revision INTEGER NOT NULL UNIQUE CHECK(created_product_revision>0),
    reviewer TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('open','decided','withdrawn')),
    body TEXT NOT NULL
);
CREATE INDEX workflow_reviews_queue ON workflow_reviews(reviewer,status,created_product_revision,id);
CREATE TABLE workflow_review_decisions (
    id TEXT PRIMARY KEY,
    review_id TEXT NOT NULL UNIQUE REFERENCES workflow_reviews(id),
    body TEXT NOT NULL
);
CREATE TABLE workflow_review_withdrawals (
    review_id TEXT PRIMARY KEY REFERENCES workflow_reviews(id),
    body TEXT NOT NULL
);

CREATE TABLE workflow_task_state (
    task_id TEXT PRIMARY KEY REFERENCES tasks(id),
    revision INTEGER NOT NULL CHECK(revision > 0),
    body TEXT NOT NULL
);
CREATE TABLE workflow_waits (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    resolver_kind TEXT NOT NULL CHECK(resolver_kind IN ('task','question','review')),
    resolver_id TEXT NOT NULL,
    closed INTEGER NOT NULL CHECK(closed IN (0,1)),
    body TEXT NOT NULL
);
CREATE INDEX workflow_waits_task ON workflow_waits(task_id,id);
CREATE INDEX workflow_waits_resolver ON workflow_waits(resolver_kind,resolver_id,closed,task_id,id);
CREATE UNIQUE INDEX workflow_waits_open_unique ON workflow_waits(task_id,resolver_kind,resolver_id) WHERE closed=0;
CREATE TABLE workflow_wait_closures (
    wait_id TEXT PRIMARY KEY REFERENCES workflow_waits(id),
    body TEXT NOT NULL
);
CREATE TABLE workflow_wait_consumptions (
    wait_id TEXT NOT NULL REFERENCES workflow_waits(id),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    attempt_id TEXT NOT NULL REFERENCES attempts(id),
    body TEXT NOT NULL,
    input_ack TEXT,
    PRIMARY KEY(wait_id,attempt_id)
);

CREATE TABLE workflow_events (
  product_revision INTEGER NOT NULL CHECK(product_revision > 0),
  ordinal INTEGER NOT NULL CHECK(ordinal >= 0 AND ordinal <= 4294967295),
  body TEXT NOT NULL,
  targets TEXT NOT NULL,
  PRIMARY KEY(product_revision, ordinal)
) WITHOUT ROWID;
ALTER TABLE sessions ADD COLUMN workflow_delivery TEXT;

INSERT INTO settings(name,value) VALUES('workflow_revision','0');
PRAGMA user_version = 13;
