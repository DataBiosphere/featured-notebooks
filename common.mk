ifndef BDCAT_NOTEBOOKS_HOME
$(error Please run "source environment" in the bdcate_notebooks repo root directory before running make commands)
endif

ifeq ($(shell which gsutil),)
$(error Please install gsutil by following instructions at "https://cloud.google.com/storage/docs/gsutil_install")
endif
