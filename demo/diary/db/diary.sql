
CREATE TABLE entry (
  id BIGINT UNSIGNED NOT NULL,
  author_id BIGINT UNSIGNED NOT NULL,
  title VARBINARY(255),
  body BLOB,
  created_on TIMESTAMP NOT NULL,
  PRIMARY KEY (id),
  KEY (author_id, created_on),
  KEY (created_on)
) DEFAULT CHARSET=BINARY;
