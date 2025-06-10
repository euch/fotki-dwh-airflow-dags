CREATE TABLE edm.ai_description (
	abs_filename varchar NOT NULL,
	caption_vit_gpt2 varchar NULL,
	CONSTRAINT ai_description_pk PRIMARY KEY (abs_filename),
	CONSTRAINT ai_description_tree_fk FOREIGN KEY (abs_filename) REFERENCES edm.tree(abs_filename) ON DELETE CASCADE ON UPDATE RESTRICT
);

CREATE TABLE edm.metadata (
	abs_filename varchar NOT NULL,
	hash varchar NOT NULL,
	exif json NULL,
	preview bytea NULL,
	CONSTRAINT metadata_pk PRIMARY KEY (abs_filename),
	CONSTRAINT metadata_fk FOREIGN KEY (abs_filename) REFERENCES edm.tree(abs_filename) ON DELETE CASCADE ON UPDATE RESTRICT
);
CREATE INDEX metadata_hash_idx ON edm.metadata USING btree (hash);