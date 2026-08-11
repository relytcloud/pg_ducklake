# Makefile for isolation tests

ROOT_DIR = ../../..

include $(ROOT_DIR)/Makefile.pgxs

# WITH_PG_DUCKDB=1 also preloads + creates community pg_duckdb, exercising the
# suite with both extensions co-installed (coexist.conf overrides the preload).
# Since 1.0.0 moved pg_ducklake's readers into the ducklake schema, the old
# public read_csv/read_parquet collision with pg_duckdb is gone.
check-isolation:
	@cd specs && find ducklake -type d -exec mkdir -p ../output_iso/results/{} \;
	TEST_DIR=$(CURDIR) $(pg_isolation_regress_installcheck) \
	--encoding=UTF8 \
	--no-locale \
	--temp-instance=./tmp_check \
	--temp-config isolation.conf \
	$(if $(WITH_PG_DUCKDB),--temp-config coexist.conf --load-extension=pg_duckdb) \
	--load-extension=pg_ducklake \
	$(if $(TEST),$(TEST),--schedule=schedule)

clean-isolation:
	rm -fr $(CURDIR)/tmp_check
	rm -fr $(CURDIR)/output_iso
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out
