@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-local-kafka.ps1" %*
