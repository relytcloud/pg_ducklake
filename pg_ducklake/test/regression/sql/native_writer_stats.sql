-- Native-writer counters are a fixed, process-shared, nontransactional set.
SELECT ducklake.reset_native_writer_stats();
SELECT event, count FROM ducklake.native_writer_stats() ORDER BY event;

CALL ducklake.set_option('data_inlining_row_limit', 100);
CREATE TABLE native_writer_stats_t (id int, value text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('native_writer_stats_t'::regclass);

INSERT INTO native_writer_stats_t VALUES (1, 'values'), (2, 'values');
COPY native_writer_stats_t FROM STDIN;
3	copy
4	copy
\.

SELECT event, count
FROM ducklake.native_writer_stats()
WHERE count <> 0
ORDER BY event;

SELECT ducklake.reset_native_writer_stats();
SELECT count(*) AS nonzero_counters
FROM ducklake.native_writer_stats()
WHERE count <> 0;

DROP TABLE native_writer_stats_t;
CALL ducklake.set_option('data_inlining_row_limit', 0);
