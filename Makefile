#
# needs python3 (>= 3.10) and python3-venv
#

TARGET = yaletomqtt
HOSTNAME = $(shell hostname)
CFG_SRC := $(if $(wildcard $(TARGET).$(HOSTNAME).cfg),$(TARGET).$(HOSTNAME).cfg,$(TARGET).cfg)
VENV = .venv

##

all: $(VENV)
$(VENV): requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements.txt
	touch $(VENV)
clean:
	rm -rf $(VENV) __pycache__
lint: $(VENV)
	$(VENV)/bin/python -m py_compile $(TARGET).py
test: $(VENV)
	$(VENV)/bin/python -u $(TARGET).py --config $(CFG_SRC) --debug true
# once: sign in with the Yale Home account at the link it prints; the tokens go to the config's token-file
authorize: $(VENV)
	$(VENV)/bin/python -u $(TARGET).py --config $(CFG_SRC) --authorize
.PHONY: all clean lint test authorize

##

DEFAULT_DIR = /etc/default
SYSTEMD_DIR = /etc/systemd/system
define install_systemd_service
	-systemctl disable $(2) 2>/dev/null || true
	cp $(1).service $(SYSTEMD_DIR)/$(2).service
	systemctl daemon-reload
	systemctl enable $(2)
	systemctl restart $(2) || echo "Warning: Failed to start $(2)"
endef
install_systemd_service: $(TARGET).service
	$(call install_systemd_service,$(TARGET),$(TARGET))
install_default: $(CFG_SRC)
	@echo "installing config from $(CFG_SRC)"
	install -m 644 $(CFG_SRC) $(DEFAULT_DIR)/$(TARGET)
install: $(VENV) install_default install_systemd_service
restart:
	systemctl restart $(TARGET)
.PHONY: install install_default install_systemd_service restart
