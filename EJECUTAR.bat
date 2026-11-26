@echo off
rem Ejecuta todo con doble clic en Windows: instala, genera los datos (si no existen) y lanza el prototipo
cd /d "%~dp0"
set PY=py
where py >nul 2>nul || set PY=python
if not exist datos (
  %PY% -m pip install -q -r requirements.txt
  %PY% generar_datos.py
  %PY% sql\generar_carga_neon.py
  %PY% mongo\generar_mongo.py
)
%PY% -u prototipo.py
pause
