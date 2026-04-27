import json, sys
sys.stdout.reconfigure(encoding='utf-8')

path = r"c:\datos\OneDrive - Maresa\Documentos\Proyectos\Forecast de ventas\forecast_Codigo\analisis_datos.ipynb"
with open(path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

src = ''.join(nb['cells'][18]['source'])

# Fix: X_sel must be defined BEFORE tscv uses it
# Current (wrong):
#   tscv    = TimeSeriesSplit(n_splits=5, test_size=max(3, len(X_sel) // (5 + 2)))
#   X_sel   = X[features_sel]
#   cv_rows = []
#   test_sz = max(3, len(X_sel) // (5 + 2))
#   print(...)
#
# Correct:
#   X_sel   = X[features_sel]
#   test_sz = max(3, len(X_sel) // (5 + 2))
#   tscv    = TimeSeriesSplit(n_splits=5, test_size=test_sz)
#   cv_rows = []
#   print(...)

old_block = (
    'tscv    = TimeSeriesSplit(n_splits=5, test_size=max(3, len(X_sel) // (5 + 2)))\n'
    'X_sel   = X[features_sel]\n'
    'cv_rows = []\n'
    '\n'
    'test_sz = max(3, len(X_sel) // (5 + 2))\n'
)

new_block = (
    'X_sel   = X[features_sel]\n'
    'test_sz = max(3, len(X_sel) // (5 + 2))\n'
    'tscv    = TimeSeriesSplit(n_splits=5, test_size=test_sz)\n'
    'cv_rows = []\n'
    '\n'
)

if old_block in src:
    src = src.replace(old_block, new_block)
    print("Fix de orden X_sel/tscv aplicado OK")
else:
    print("BLOQUE NO ENCONTRADO - buscando variantes...")
    # Find the lines around tscv
    lines = src.split('\n')
    for i, l in enumerate(lines):
        if 'TimeSeriesSplit' in l and 'tscv' in l:
            print(f"  Linea {i}: {repr(l)}")
        if 'X_sel' in l and 'X[features_sel]' in l:
            print(f"  Linea {i}: {repr(l)}")
        if 'test_sz' in l:
            print(f"  Linea {i}: {repr(l)}")

nb['cells'][18]['source'] = [l + '\n' for l in src.split('\n')[:-1]] + [src.split('\n')[-1]]

with open(path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Guardado OK")
# Verify
idx = src.find('X_sel   = X[features_sel]')
print("Verificacion (contexto alrededor de X_sel=X[features_sel]):")
print(src[max(0,idx-100):idx+200])
