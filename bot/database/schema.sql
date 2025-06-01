CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    is_active INTEGER DEFAULT 0,
    is_topic_enabled INTEGER DEFAULT 0,
    max_number INTEGER DEFAULT 100000000000,
    min_number INTEGER DEFAULT 0,
    max_ayat INTEGER DEFAULT 100,
    min_ayat INTEGER DEFAULT 1,
    sepas_enabled INTEGER DEFAULT 1,
    delete_after INTEGER DEFAULT 0,
    lock_enabled INTEGER DEFAULT 0,
    reset_daily INTEGER DEFAULT 0,
    stop_number INTEGER DEFAULT 0,
    time_off_start TEXT DEFAULT '',
    time_off_end TEXT DEFAULT '',
    show_total INTEGER DEFAULT 0,
    max_display_verses INTEGER DEFAULT 10,
    min_display_verses INTEGER DEFAULT 1,
    invite_link TEXT DEFAULT '',
    title TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS topics (
    group_id INTEGER,
    name TEXT, 
    topic_id INTEGER,
    khatm_type TEXT NOT NULL CHECK(khatm_type IN ('ghoran', 'salavat', 'zekr')),
    zekr_text TEXT DEFAULT '',
    current_total INTEGER DEFAULT 0,
    period_number INTEGER DEFAULT 0,
    reset_on_period INTEGER DEFAULT 0,
    max_ayat INTEGER DEFAULT 100,
    min_ayat INTEGER DEFAULT 1,
    stop_number INTEGER DEFAULT 0,
    completion_message TEXT DEFAULT '',
    completion_count INTEGER DEFAULT 0,
    current_verse_id INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    is_completed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_id, topic_id),
    FOREIGN KEY (group_id) REFERENCES groups(group_id)
);

CREATE TABLE IF NOT EXISTS khatm_ranges (
    group_id INTEGER,
    topic_id INTEGER,
    start_verse_id INTEGER NOT NULL,
    end_verse_id INTEGER NOT NULL CHECK(end_verse_id > start_verse_id),
    PRIMARY KEY (group_id, topic_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES topics(group_id, topic_id)
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    group_id INTEGER,
    topic_id INTEGER,
    username TEXT NOT NULL,
    first_name TEXT,
    total_salavat INTEGER DEFAULT 0,
    total_zekr INTEGER DEFAULT 0,
    total_ayat INTEGER DEFAULT 0,
    PRIMARY KEY (user_id, group_id, topic_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES topics(group_id, topic_id)
);

CREATE TABLE IF NOT EXISTS contributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    group_id INTEGER,
    topic_id INTEGER,
    amount INTEGER,
    verse_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id, group_id, topic_id) REFERENCES users(user_id, group_id, topic_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES topics(group_id, topic_id)
);

CREATE TABLE IF NOT EXISTS sepas_texts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER,
    text TEXT NOT NULL,
    is_default INTEGER DEFAULT 0,
    FOREIGN KEY (group_id) REFERENCES groups(group_id)
);

CREATE TABLE IF NOT EXISTS hadith_settings (
    group_id INTEGER PRIMARY KEY,
    hadith_enabled INTEGER DEFAULT 0,
    FOREIGN KEY (group_id) REFERENCES groups(group_id)
);

CREATE TABLE IF NOT EXISTS tag_timestamps (
    group_id INTEGER PRIMARY KEY,
    last_tag_time TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES groups(group_id)
);

CREATE TABLE IF NOT EXISTS banned_groups (
    group_id INTEGER PRIMARY KEY,
    banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES groups(group_id)
);
CREATE TABLE IF NOT EXISTS banned_users (
    user_id INTEGER PRIMARY KEY,
    banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_contributions_group_topic ON contributions(group_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_users_group_topic ON users(group_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_topics_group_topic ON topics(group_id, topic_id);
CREATE INDEX IF NOT EXISTS idx_khatm_ranges_group_topic ON khatm_ranges(group_id, topic_id);


CREATE TRIGGER IF NOT EXISTS update_topics_timestamp
AFTER UPDATE ON topics
FOR EACH ROW
BEGIN
    UPDATE topics SET updated_at = CURRENT_TIMESTAMP
    WHERE group_id = OLD.group_id AND topic_id = OLD.topic_id;
END;