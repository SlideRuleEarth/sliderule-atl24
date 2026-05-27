ROOT = $(shell pwd)
BUILD = $(ROOT)/build
STAGE = $(ROOT)/stage
VERSION ?= latest
SLIDERULE ?= $(ROOT)/../sliderule
ATL24 ?= $(ROOT)/../atl24_v2_algorithms
BUCKET ?= s3://sliderule
CONTAINER_REGISTRY ?= 742127912612.dkr.ecr.us-west-2.amazonaws.com
MAKECFG ?= -DCMAKE_CXX_COMPILER=gcc14-g++
USERCFG ?=

all:
	make -j8 -C $(BUILD)

config: prep
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Release -DATL24DIR=$(ATL24) $(USERCFG) $(MAKECFG) $(ROOT)

config-stage-debug: prep
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Debug -DINSTALLDIR=$(SLIDERULE)/stage/sliderule -DATL24DIR=$(ATL24) $(USERCFG) $(MAKECFG) $(ROOT)

config-stage-release: prep
	cd $(BUILD) && \
	cmake -DCMAKE_BUILD_TYPE=Release -DINSTALLDIR=$(SLIDERULE)/stage/sliderule -DATL24DIR=$(ATL24) $(USERCFG) $(MAKECFG) $(ROOT)

install:
	make -C $(BUILD) install

uninstall:
	xargs rm < $(BUILD)/install_manifest.txt

prep:
	mkdir -p $(BUILD)

tag:
	echo $(VERSION) > $(ROOT)/version.txt
	git add $(ROOT)/version.txt
	git commit -m "Version $(VERSION)"
	git tag -a $(VERSION) -m "Version $(VERSION)"
	git push --tags && git push
	gh release create $(VERSION) -t $(VERSION) --notes "see https://slideruleearth.io for details"

release: distclean tag config-stage-release all publish

atl24d-docker:
	-rm -Rf $(STAGE)
	mkdir -p $(STAGE)
	cd docker && conda-lock -p linux-$(shell arch) -f environment.yml
	cd docker && conda-lock render -p linux-$(shell arch)
	cp docker/Dockerfile $(STAGE)
	cp docker/conda-* $(STAGE)
	cp docker/runner.* $(STAGE)
	cd $(STAGE) && docker build -t $(CONTAINER_REGISTRY)/atl24d:$(VERSION) .

atl24d-push:
	docker push $(CONTAINER_REGISTRY)/atl24d:$(VERSION)

atl24-docker:
	-rm -Rf $(STAGE)
	mkdir -p $(STAGE)
	rsync -a $(ROOT) $(STAGE) --exclude build --exclude stage --exclude data
	rsync -a $(SLIDERULE) $(STAGE) --exclude build --exclude stage
	rsync -a $(ATL24) $(STAGE) --exclude build --exclude stage
	cp docker/atl24/Dockerfile $(STAGE)
	cp docker/atl24/docker-entrypoint.sh $(STAGE)
	cd $(STAGE) && docker build --build-arg repo=$(CONTAINER_REGISTRY) -t $(CONTAINER_REGISTRY)/sliderule:atl24 .
	docker tag $(CONTAINER_REGISTRY)/sliderule:atl24 $(CONTAINER_REGISTRY)/sliderule:unstable

clean:
	- make -C $(BUILD) clean

distclean:
	- rm -Rf $(BUILD)


