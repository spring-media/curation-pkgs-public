GODIRS = pkg/cloudwatchmetrics pkg/csvexport

update_go_deps: $(GODIRS)

.PHONY: update_go_deps $(GODIRS)

$(GODIRS):
	$(MAKE) -C $@ update_deps
