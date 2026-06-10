CREATE TABLE agent_refs (
  agent_id   text PRIMARY KEY,
  head       text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE memory_jobs (
  id         bigserial PRIMARY KEY,
  agent_id   text NOT NULL,
  kind       text NOT NULL,
  from_sha   text,
  state      text NOT NULL DEFAULT 'pending',
  attempts   int  NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- per-agent singleton: at most one pending job per (agent_id, kind).
CREATE UNIQUE INDEX memory_jobs_pending_uniq
  ON memory_jobs (agent_id, kind) WHERE state = 'pending';

CREATE TABLE maintenance_cursor (
  agent_id      text NOT NULL,
  kind          text NOT NULL,
  processed_sha text NOT NULL,
  PRIMARY KEY (agent_id, kind)
);
