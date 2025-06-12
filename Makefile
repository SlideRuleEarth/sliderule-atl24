ROOT = $(shell pwd)
BUILD = $(ROOT)/build
VERSION ?= latest
SLIDERULE ?= $(ROOT)/../sliderule
BUCKET ?= s3://sliderule/config/
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

publish: ## upload plugin to slideruleearth plugin bucket
	aws s3 cp $(BUILD)/atl24.so $(BUCKET)

stage:
	mkdir -p $(SLIDERULE)/stage/sliderule/lib/sliderule/
	cp $(BUILD)/atl24.so $(SLIDERULE)/stage/sliderule/lib/sliderule/

tag: ## create version tag in this repository and release it on GitHub
	echo $(VERSION) > $(ROOT)/version.txt
	git add $(ROOT)/version.txt
	git commit -m "Version $(VERSION)"
	git tag -a $(VERSION) -m "Version $(VERSION)"
	git push --tags && git push
	gh release create $(VERSION) -t $(VERSION) --notes "see https://slideruleearth.io for details"

release: distclean tag config-release all publish ## release a version of atl24 plugin; needs VERSION

clean: ## clean last build
	- make -C $(BUILD) clean

distclean: ## fully remove all non-version controlled files and directories
	- rm -Rf $(BUILD)

help: ## that's me!
	@grep -E '^[a-zA-Z_-].+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

