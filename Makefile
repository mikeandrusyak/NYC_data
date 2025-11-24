PYTHON_VERSION := 3.12.2
VENV_DIR := .venv
DAW := .venv/bin/daw-run
UNAME_S := $(shell uname -s 2>/dev/null || echo Windows_NT)


ifeq ($(UNAME_S),Linux)
	OS := linux
	SHELL := /bin/bash
	SHELLFLAGS := -o pipefail -c
else ifeq ($(UNAME_S),Darwin)
	OS := mac
	SHELL := /bin/bash
	SHELLFLAGS := -o pipefail -c
	RUN_DAW := $(VENV_DIR)/bin/daw-run
else
	OS := windows
	SHELL := powershell.exe
	SHELLFLAGS := -NoProfile -ExecutionPolicy Bypass -Command
	RUN_DAW := $(VENV_DIR)\Scripts\daw-run
endif

ifeq ($(OS), windows)

install-python-version:
	if (Get-Command pyenv -ErrorAction SilentlyContinue) { \
		if (-not (pyenv versions | Select-String "$(PYTHON_VERSION)")) { \
			Write-Host "Python $(PYTHON_VERSION) nicht installiert, installiere via pyenv..."; \
			pyenv install $(PYTHON_VERSION); \
		} else { \
			Write-Host "Python $(PYTHON_VERSION) bereits via pyenv installiert."; \
		}; \
		pyenv local $(PYTHON_VERSION); \
	} else { \
		Write-Host "pyenv nicht gefunden, installiere Python ueber winget..."; \
		if (Get-Command winget -ErrorAction SilentlyContinue) { \
			winget install -e --id Python.Python.3.12; \
		} else { \
			Write-Host "winget nicht gefunden. Bitte Python $(PYTHON_VERSION) manuell installieren."; \
			exit 1; \
		}; \
	}

venv: install-python-version
	if (Test-Path "$(VENV_DIR)") { Write-Host "venv schon vorhanden."; } else { python -m venv .venv }
	Write-Host "Starte Venv";
	.\.venv\Scripts\Activate.ps1

install: venv
	Write-Host "Installiere Dependencies...";
	$(VENV_DIR)\Scripts\python.exe -m pip install --upgrade pip
	$(VENV_DIR)\Scripts\python.exe -m pip install -e .[dev]
	Write-Host "Dependencies installiert";

start-up: 
	$(RUN_DAW)

all-win: install
	Write-Host "Setup Done"

else 

install-python-version:
	@if command -v pyenv >/dev/null 2>&1; then \
		echo "pyenv gefunden. Verwende pyenv."; \
		if ! pyenv versions --bare | grep -qx "$(PYTHON_VERSION)"; then \
			echo "Installiere Python $(PYTHON_VERSION) via pyenv..."; \
			pyenv install $(PYTHON_VERSION); \
		else \
			echo "Python $(PYTHON_VERSION) ist bereits in pyenv installiert."; \
		fi; \
		pyenv local $(PYTHON_VERSION); \
	else \
		echo "pyenv wurde nicht gefunden. Verwende System-Installation."; \
		NEED_INSTALL=0; \
		if command -v python3 >/dev/null 2>&1; then \
			SYS_VER="$$(python3 -c 'import sys; print(\".\".join(map(str, sys.version_info[:3])))')"; \
			if [ "$$SYS_VER" = "$(PYTHON_VERSION)" ]; then \
				echo "System-Python-Version stimmt bereits: $$SYS_VER"; \
			else \
				echo "System-Python-Version ist $$SYS_VER, erwartet wird $(PYTHON_VERSION)."; \
				NEED_INSTALL=1; \
			fi; \
		else \
			echo "python3 nicht gefunden."; \
			NEED_INSTALL=1; \
		fi; \
		if [ "$$NEED_INSTALL" = "1" ]; then \
			echo "Installiere Python $(PYTHON_VERSION) ueber Homebrew..."; \
			if command -v brew >/dev/null 2>&1; then \
				brew install python@$(PYTHON_VERSION); \
				brew link python@$(PYTHON_VERSION) --force --overwrite; \
			else \
				echo "Homebrew nicht gefunden. Installation nicht moeglich."; \
				exit 1; \
			fi; \
		fi; \
	fi

venv: install-python-version
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "🐍 Erzeuge neue venv..."; \
		"$$(pyenv which python)" -m venv "$(VENV_DIR)"; \
	else \
		echo "✅ venv existiert bereits."; \
	fi

install: venv
	"$(VENV_DIR)/bin/python" -m pip install --upgrade pip
	"$(VENV_DIR)/bin/python" -m pip install -e .[dev]
	@echo "✅ Dependencies installiert."

all-mac: install

start-up:
	$(RUN_DAW)

endif