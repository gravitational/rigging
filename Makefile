# all goinstalls everything
.PHONY: all
all: install

# install installs binary
.PHONY: install
install: 
	go install github.com/gravitational/rigging/tool/rig

.PHONY: build
build:
	go build ./...
