
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, io, json, zipfile, importlib.util, re
from typing import Dict
import pandas as pd

from content import run_flow

# ---- USER SETTINGS ----
FLOW_PATH   = "AdmitDeclineCleanFlow.tfl"   # .tfl or .tflx (packaged)
DATA_DIR    = "./Original Datasets"                      # one folder with ALL Excel inputs
SCRIPTS_DIR = "./Scripts"                   # one folder with ALL Python scripts used by the flow
OUTDIR      = "./prep_outputs"

# ---- Helpers ----
def _load_flow_json(p):
    with open(p,'rb') as fh:
        head = fh.read(4)
    if head == b'PK\x03\x04':
        with zipfile.ZipFile(p,'r') as z:
            with z.open('flow','r') as f:
                return json.load(io.TextIOWrapper(f, encoding='utf-8'))
    else:
        return json.load(open(p,'r',encoding='utf-8'))

def build_filename_map_from_folder(flow_path: str, folder: str) -> Dict[str,str]:
    """Map each original Excel path in the flow to a file with the same basename under folder."""
    fj = _load_flow_json(flow_path)
    conns = fj.get('connections', {})
    nodes = fj.get('nodes', {})
    wanted = set()
    for node in nodes.values():
        if node.get('nodeType') == '.v2022_4_1.LoadExcel':
            conn = conns.get(node.get('connectionId'), {})
            attrs = (conn.get('connectionAttributes') or {})
            fn = attrs.get('filename')
            if fn:
                wanted.add(fn)

    # index folder by basename
    idx = {}
    for root, _dirs, files in os.walk(folder):
        for f in files:
            if f.lower().endswith((".xlsx",".xls")):
                idx.setdefault(f.lower(), []).append(os.path.join(root, f))

    fmap = {}
    for orig in wanted:
        base = os.path.basename(orig).lower()
        if base in idx:
            fmap[orig] = idx[base][0]
        else:
            print(f"[warn] Could not find {base} under {folder}")
    return fmap

def _load_py_func_from_dir(dir_path: str, basename: str, func_name: str):
    """Load function func_name from file with given basename inside dir_path."""
    file_path = os.path.join(dir_path, basename)
    if not os.path.exists(file_path):
        # Try common case-insensitive match
        lower = basename.lower()
        for f in os.listdir(dir_path):
            if f.lower() == lower:
                file_path = os.path.join(dir_path, f)
                break
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Script file not found in {dir_path}: {basename}")
    spec = importlib.util.spec_from_file_location("prep_script_mod", file_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    fn = getattr(mod, func_name, None)
    if fn is None:
        raise AttributeError(f"Function {func_name} not found in {file_path}")
    return fn

def my_extensibility_hook(df: pd.DataFrame, info: dict) -> pd.DataFrame:
    """
    Dispatch Script steps by reading:
      - setup.scriptFilePath  -> basename to locate under SCRIPTS_DIR
      - exec.scriptFunctionName -> function to call
    """
    setup, execp = info.get("setup", {}), info.get("exec", {})
    script_path = setup.get("scriptFilePath") or setup.get("pythonFile") or ""
    func_name   = execp.get("scriptFunctionName") or execp.get("functionName") or "main"
    basename = os.path.basename(script_path) if script_path else None

    if not basename:
        # Fallback by step name -> known files (edit as needed)
        fallback = {
            "stopwords": "stopwords.py",
            "uni_names": "uni_names.py",
            "unisplit": "unisplit.py",
            "replace": "replace.py",
            "add_location": "cleaning-script.py",
        }
        key = func_name.lower()
        basename = fallback.get(key)

    if not basename:
        print(f"[script] No basename for step '{info.get('name')}', passing through.")
        return df

    try:
        fn = _load_py_func_from_dir(SCRIPTS_DIR, basename, func_name)
        out = fn(df)
        return out
    except Exception as e:
        print(f"[script] Error in {basename}:{func_name}: {e}. Passing through.")
        return df

if __name__ == "__main__":
    os.makedirs(OUTDIR, exist_ok=True)
    # Build map from a single folder
    FILENAME_MAP = build_filename_map_from_folder(FLOW_PATH, DATA_DIR)
    # Run the flow
    cache = run_flow(
        FLOW_PATH,
        outdir=OUTDIR,
        filename_map=FILENAME_MAP,
        extensibility_hook=my_extensibility_hook
    )
    print("✅ Done. Outputs in", os.path.abspath(OUTDIR))
