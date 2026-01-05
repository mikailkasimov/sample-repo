-- Add up migration script here
CREATE TABLE reddit_posts( 
    subreddit_id TEXT NOT NULL,
    name TEXT NOT NULL, 
    title TEXT,
    text TEXT,
    flair TEXT, 
    author_fullname TEXT NOT NULL, 
    url TEXT, 
    media JSONB, 
    created_utc TIMESTAMP, 
    data JSONB NOT NULL
);

CREATE TABLE reddit_comments( 
    subreddit_id TEXT NOT NULL,
    name TEXT NOT NULL, 
    text TEXT, 
    author_fullname TEXT NOT NULL, 
    link_id TEXT NOT NULL,
    parent_id TEXT NOT NULL, 
    created_utc TIMESTAMP, 
    data JSONB NOT NULL 
);

CREATE TABLE chan_posts(
    board_name TEXT NOT NULL,
    thread_number BIGINT NOT NULL,
    post_number BIGINT NOT NULL,
    title TEXT,
    text_body TEXT,
    num_replies BIGINT,
    created_utc TIMESTAMP NOT NULL,
    resto BIGINT,
    data JSONB NOT NULL

);

CREATE UNIQUE INDEX ON reddit_posts (name, created_utc);
CREATE UNIQUE INDEX ON reddit_comments (name, created_utc);
CREATE UNIQUE INDEX ON chan_posts(board_name, post_number, created_utc);


SELECT create_hypertable('reddit_posts', 'created_utc', chunk_time_interval => INTERVAL '1 hours');
SELECT create_hypertable('reddit_comments', 'created_utc', chunk_time_interval => INTERVAL '1 hours');
SELECT create_hypertable('chan_posts', 'created_utc', chunk_time_interval => INTERVAL '1 hours');
