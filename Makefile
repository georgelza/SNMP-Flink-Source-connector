.DEFAULT_GOAL := help

define HELP

Available commands:

- build: Maven Build the Project/jar's.

- clean: Remove all created target directories

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


build:
	mvn clean package

clean:
	cd snmp-job; rm -rf target
	cd snmp-job; rm dependency-reduced-pom.xml
	cd snmp-source; rm -rf target
	cd snmp-mib-loader; rm -rf target
