ROOT = $(shell pwd)
BUILD = $(ROOT)/build
STAGE = $(ROOT)/stage
VERSION ?= latest
SLIDERULE ?= $(ROOT)/../sliderule
ATL24 ?= $(ROOT)/../atl24_v2_algorithms
BUCKET ?= s3://sliderule
CONTAINER_REGISTRY ?= 742127912612.dkr.ecr.us-west-2.amazonaws.com
ENVVER ?= $(shell git describe --abbrev --dirty --always --tags --long)
PROJECT_BUCKET = sliderule
PROJECT_FOLDER = cf
AWS_REGION = us-west-2
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

selftest: install
	make -C $(SLIDERULE)/targets/slideruleearth run RUN_CMD=/home/jswinski/meta/sliderule-atl24/selftests/atl24_writer.lua

tag:
	echo $(VERSION) > $(ROOT)/version.txt
	git add $(ROOT)/version.txt
	git commit -m "Version $(VERSION)"
	git tag -a $(VERSION) -m "Version $(VERSION)"
	git push --tags && git push
	gh release create $(VERSION) -t $(VERSION) --notes "see https://slideruleearth.io for details"

release: distclean tag config-stage-release all publish

docker-runner:
	-rm -Rf $(STAGE)
	mkdir -p $(STAGE)
	rsync -a $(ROOT) $(STAGE) --exclude build --exclude stage --exclude data
	rsync -a $(SLIDERULE) $(STAGE) --exclude build --exclude stage
	rsync -a $(ATL24) $(STAGE) --exclude build --exclude stage
	cp docker/atl24/Dockerfile $(STAGE)
	cp docker/atl24/docker-entrypoint.sh $(STAGE)
	cd $(STAGE) && docker build --build-arg repo=$(CONTAINER_REGISTRY) -t $(CONTAINER_REGISTRY)/sliderule:runner .

test-docker-run:
	docker run \
		--network host \
		-v /data:/data \
		-v $(ROOT):$(ROOT) \
		-e IPV4=localhost \
		-e LOG_FORMAT=FMT_TEXT \
		-e ENVIRONMENT_VERSION=$(ENVVER) \
		-e PROJECT_BUCKET=$(PROJECT_BUCKET) \
		-e PROJECT_FOLDER=$(PROJECT_FOLDER) \
		-e PROJECT_REGION=$(AWS_REGION) \
		-e ORCHESTRATOR=http://127.0.0.1:8050 \
		-e CLUSTER=localhost \
		-e DOMAIN=localhost \
		-e AMS=http://127.0.0.1:9082 \
		-e CONTAINER_REGISTRY=$(CONTAINER_REGISTRY) \
		--name atl24 --rm \
		$(CONTAINER_REGISTRY)/sliderule:runner \
		/usr/local/etc/sliderule/job_runner.lua $(ROOT)/utils/gen_atl24r3.lua ATL03_20181028071900_04530107_006_02.h5 /tmp

test-atl24-run: install
#	make -C $(SLIDERULE)/targets/slideruleearth job ARGS="$(ROOT)/utils/gen_atl24r3.lua ATL03_20181028071900_04530107_006_02.h5 /tmp"
	make -C $(SLIDERULE)/targets/slideruleearth job ARGS="$(ROOT)/utils/gen_atl24r3.lua ATL03_20191215112656_12150507_006_01.h5 /tmp"

clean:
	- make -C $(BUILD) clean

distclean:
	- rm -Rf $(BUILD)
