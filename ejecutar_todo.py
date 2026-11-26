"""Prepara el entorno y ejecuta el prototipo completo (doble clic o F5 en IDLE)."""
import os, subprocess, sys, runpy, traceback
os.chdir(os.path.dirname(os.path.abspath(__file__)))
py = sys.executable.replace("pythonw.exe", "python.exe")
if not os.path.isdir("datos"):
    print("Instalando dependencias (duckdb, deltalake, pyarrow)...")
    r = subprocess.run([py, "-m", "pip", "install", "-q", "-r", "requirements.txt"], capture_output=True, text=True)
    print(r.stdout[-800:], r.stderr[-800:])
    for paso in (["generar_datos.py"], ["sql/generar_carga_neon.py"], ["mongo/generar_mongo.py"]):
        r = subprocess.run([py] + paso, capture_output=True, text=True)
        print(r.stdout, r.stderr[-1500:])
try:
    runpy.run_path("prototipo.py", run_name="__main__")
except BaseException:
    with open("error_prototipo.txt", "w", encoding="utf-8") as f:
        traceback.print_exc(file=f)
    traceback.print_exc()
try:
    input("\nFin. Pulsa Enter para cerrar esta ventana...")
except Exception:
    pass
