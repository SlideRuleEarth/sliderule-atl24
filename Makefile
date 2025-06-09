ROOT = $(shell pwd)
BUILD = $(ROOT)/build
USERCFG ?=

all: ## build code
	make -j8 -C $(BUILD)

config: prep ## configure make for debug version of sliderule
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Release $(USERCFG) $(ROOT)

config-release: prep ## configure make for release version of sliderule
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Release $(USERCFG) $(ROOT)

install: ## install sliderule to system
	make -C $(BUILD) install

uninstall: ## uninstall most recent install of sliderule from system
	xargs rm < $(BUILD)/install_manifest.txt

prep: ## create necessary build directories
	mkdir -p $(BUILD)

clean: ## clean last build
	- make -C $(BUILD) clean

distclean: ## fully remove all non-version controlled files and directories
	- rm -Rf $(BUILD)

help: ## that's me!
	@grep -E '^[a-zA-Z_-].+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

