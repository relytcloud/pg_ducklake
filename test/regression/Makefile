# Makefile for regression test

ROOT_DIR = ../..

include $(ROOT_DIR)/Makefile.global

TESTDATA_DIR = /tmp/pg_ducklake_testdata

$(CURDIR)/data/titanic.csv:
	mkdir -p $(CURDIR)/data
	curl -fSL -o $@ 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'

dataset: $(CURDIR)/data/titanic.csv
	mkdir -p $(TESTDATA_DIR)
	cp -f $(CURDIR)/data/* $(TESTDATA_DIR)/

check-regression: dataset
	TEST_DIR=$(CURDIR) $(pg_regress_installcheck) \
	--encoding=UTF8 \
	--no-locale \
	--temp-instance=./tmp_check \
	--temp-config regression.conf \
	--load-extension=pg_duckdb \
	--load-extension=pg_ducklake \
	$(if $(TEST),$(TEST),--schedule=schedule)

clean-regression:
	rm -rf $(TESTDATA_DIR)
	rm -fr $(CURDIR)/tmp_check
	rm -fr $(CURDIR)/log
	rm -fr $(CURDIR)/results
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out
