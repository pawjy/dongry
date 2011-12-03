CREATE TABLE category (
  id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name_ja VARCHAR(255),
  name_en VARCHAR(255),
  name_fr VARCHAR(255),
  created_on TIMESTAMP NOT NULL DEFAULT 0,
  updated_on TIMESTAMP NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY (name_ja, updated_on),
  KEY (name_en, updated_on),
  KEY (name_fr, updated_on),
  KEY (created_on),
  KEY (updated_on)
) DEFAULT CHARSET=BINARY, ENGINE=InnoDB;

CREATE TABLE entry (
  category_id INT UNSIGNED NOT NULL,
  entry_id INT UNSIGNED NOT NULL,
  title_ja VARCHAR(255),
  title_en VARCHAR(255),
  title_fr VARCHAR(255),
  created_on TIMESTAMP NOT NULL DEFAULT 0,
  updated_on TIMESTAMP NOT NULL DEFAULT 0,
  PRIMARY KEY (category_id, entry_id),
  KEY (category_id, title_ja, updated_on),
  KEY (category_id, title_en, updated_on),
  KEY (category_id, title_fr, updated_on),
  KEY (created_on),
  KEY (updated_on)
) DEFAULT CHARSET=BINARY, ENGINE=InnoDB;

CREATE TABLE description (
  category_id INT UNSIGNED NOT NULL,
  entry_id INT UNSIGNED NOT NULL,
  lang CHAR(2) NOT NULL,
  text BLOB NOT NULL,
  metadata BLOB NOT NULL,
  created_on TIMESTAMP NOT NULL DEFAULT 0,
  updated_on TIMESTAMP NOT NULL DEFAULT 0,
  PRIMARY KEY (category_id, entry_id, lang),
  KEY (created_on),
  KEY (updated_on)
) DEFAULT CHARSET=BINARY, ENGINE=InnoDB;
