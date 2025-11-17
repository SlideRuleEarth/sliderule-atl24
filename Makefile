ROOT = $(shell pwd)
BUILD = $(ROOT)/build
STAGE = $(ROOT)/stage
VERSION ?= latest
SLIDERULE ?= $(ROOT)/../sliderule
ATL24 ?= $(ROOT)/../atl24_v2_algorithms
BUCKET ?= s3://sliderule
ECR ?= 742127912612.dkr.ecr.us-west-2.amazonaws.com
USERCFG ?=

all:
	make -j8 -C $(BUILD)

config: prep
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Release -DATL24DIR=$(ATL24) $(USERCFG) $(ROOT)

config-stage-debug: prep
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Debug -DINSTALLDIR=$(SLIDERULE)/stage/sliderule -DATL24DIR=$(ATL24) $(USERCFG) $(ROOT)

config-stage-release: prep
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Release -DINSTALLDIR=$(SLIDERULE)/stage/sliderule -DATL24DIR=$(ATL24) $(USERCFG) $(ROOT)

install:
	make -C $(BUILD) install

uninstall:
	xargs rm < $(BUILD)/install_manifest.txt

prep:
	mkdir -p $(BUILD)

publish:
	aws s3 cp $(BUILD)/atl24.so $(BUCKET)/plugins/
	aws s3 cp endpoints/atl24g2.lua $(BUCKET)/plugins/api/

tag:
	echo $(VERSION) > $(ROOT)/version.txt
	git add $(ROOT)/version.txt
	git commit -m "Version $(VERSION)"
	git tag -a $(VERSION) -m "Version $(VERSION)"
	git push --tags && git push
	gh release create $(VERSION) -t $(VERSION) --notes "see https://slideruleearth.io for details"

release: distclean tag config-stage-release all publish

atl24d-lock:
	cd docker && conda-lock -p linux-$(shell arch) -f environment.yml
	cd docker && conda-lock render -p linux-$(shell arch)

atl24d-docker:
	-rm -Rf $(STAGE)
	mkdir -p $(STAGE)
	cp docker/Dockerfile $(STAGE)
	cp docker/conda-* $(STAGE)
	cp docker/runner.* $(STAGE)
	cd $(STAGE) && docker build -t $(ECR)/atl24d:$(VERSION) .

atl24d-push:
	docker push $(ECR)/atl24d:$(VERSION)

atl24d-docker: atl24d-lock atl24d-docker atl24d-push

database-export:
	aws s3 cp data/atl24r2.db s3://sliderule/cf/

clean:
	- make -C $(BUILD) clean

distclean:
	- rm -Rf $(BUILD)


