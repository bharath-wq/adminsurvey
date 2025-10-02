#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
prep2py_tableau2022: Execute Tableau Prep "flow" JSON (from .tfl/.tflx) with pandas.

Supported nodes in this tailored build (extend as needed):
- .v2022_4_1.LoadExcel   -> pandas.read_excel
- .v2019_1_4.Remap       -> replace listed values with NaN (acts like cleaning nulls / recodes)
- .v2019_1_4.ChangeType  -> pandas dtype casting
- .v2019_2_2.SuperExtensibilityNode -> optional external Python function (skipped if unavailable)
- .v2018_2_3.SuperUnion  -> pandas.concat (align columns)
- .v1.Container          -> executes nested loomContainer subgraph
- .v1.WriteToHyper       -> write CSV instead (Hyper not supported here)

Notes:
- Excel file paths are taken from connections[*].connectionAttributes.filename and sheet from node.relation.table (e.g., "['SheetName$']").
- If a file isn't present locally, set filename_map in run_flow(...).
- External script steps (Extensibility) are optional; provide a python call hook in run_flow if you'd like to run them.
"""

from __future__ import annotations

import io
import json
import os
import re
import zipfile
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class Node:
    id: str
    name: str
    nodeType: str
    baseType: str
    nextNodes: List[Tuple[str, str]]  # list of (nextNodeId, nextNamespace)
    config: Dict[str, Any]


@dataclass
class Flow:
    nodes: Dict[str, Node]
    initial: List[str]
    connections: Dict[str, Dict[str, Any]]


# ----------------- Load .tfl/.tflx -----------------


def load_flow(path: str) -> Dict[str, Any]:
    with open(path, "rb") as fh:
        head = fh.read(4)
    if head == b"PK\x03\x04":
        with zipfile.ZipFile(path, "r") as z:
            # Tableau Packaged Flow: main JSON is 'flow'
            with z.open("flow", "r") as f:
                return json.load(io.TextIOWrapper(f, encoding="utf-8"))
    else:
        # raw JSON
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def _parse_next(nextNodes: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    out = []
    for n in nextNodes or []:
        nid = n.get("nextNodeId")
        ns = n.get("nextNamespace") or n.get("namespace") or "Default"
        if nid:
            out.append((nid, ns))
    return out


def parse(flow_json: Dict[str, Any]) -> Flow:
    nodes_raw = flow_json.get("nodes", {})
    nodes: Dict[str, Node] = {}
    for nid, obj in nodes_raw.items():
        nodes[nid] = Node(
            id=obj.get("id") or nid,
            name=obj.get("name") or nid,
            nodeType=obj.get("nodeType") or obj.get("type") or "Unknown",
            baseType=obj.get("baseType") or "transform",
            nextNodes=_parse_next(obj.get("nextNodes", [])),
            config=obj,
        )
    initial = list(flow_json.get("initialNodes") or [])
    connections = flow_json.get("connections") or {}
    return Flow(nodes=nodes, initial=initial, connections=connections)


# ----------------- Execution -----------------


class Context:
    def __init__(
        self,
        flow_path: str,
        outdir: str,
        filename_map: Optional[Dict[str, str]] = None,
        extensibility_hook: Optional[
            Callable[[pd.DataFrame, Dict[str, Any]], pd.DataFrame]
        ] = None,
    ):
        self.flow_path = os.path.abspath(flow_path)
        self.flow_dir = os.path.dirname(self.flow_path)
        self.outdir = os.path.abspath(outdir)
        os.makedirs(self.outdir, exist_ok=True)
        self.filename_map = filename_map or {}
        self.extensibility_hook = extensibility_hook
        self.cache: Dict[str, pd.DataFrame] = {}
        self.warnings: List[str] = []


def _excel_sheet_from_table(table_str: str) -> Optional[str]:
    # pattern like "['SheetName$']"
    if not table_str:
        return None
    m = re.search(r"\['(.+?)'\]", table_str)
    if m:
        val = m.group(1)
        # strip trailing $ if present
        if val.endswith("$"):
            val = val[:-1]
        return val
    return None


def _exec_LoadExcel(node: Node, flow: Flow, ctx: Context) -> pd.DataFrame:
    conn_id = node.config.get("connectionId")
    conn = flow.connections.get(conn_id) or {}
    attrs = conn.get("connectionAttributes") or {}
    filename = attrs.get("filename")
    # allow mapping to local path
    filename = ctx.filename_map.get(filename, filename)
    if not filename:
        raise FileNotFoundError(
            f"Input '{node.name}' has no filename in connection {conn_id}"
        )
    if not os.path.isabs(filename):
        filename = os.path.join(ctx.flow_dir, filename)
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Excel not found for '{node.name}': {filename}")
    # sheet
    table = (node.config.get("relation") or {}).get("table")
    sheet = _excel_sheet_from_table(table) or 0
    df = pd.read_excel(filename, sheet_name=sheet)
    return df


def _exec_Remap(df: pd.DataFrame, node: Node) -> pd.DataFrame:
    d = df.copy()
    col = node.config.get("columnName")
    val_map = (node.config.get("values") or {}).get("null") or []
    if col and col in d.columns and val_map:
        # normalize entries like '""' or 'null' or specific strings -> NaN
        targets = [
            v.strip('"').strip("'") if isinstance(v, str) else v for v in val_map
        ]
        d[col] = d[col].replace(targets, np.nan)
    return d


def _exec_ChangeType(df: pd.DataFrame, node: Node) -> pd.DataFrame:
    d = df.copy()
    col = node.config.get("columnName")
    to = (node.config.get("dataType") or "").lower()
    if col in d.columns:
        if to in ("string", "text"):
            d[col] = d[col].astype("string")
        elif to in ("integer", "int"):
            d[col] = pd.to_numeric(d[col], errors="coerce").astype("Int64")
        elif to in ("float", "double", "number", "real", "decimal"):
            d[col] = pd.to_numeric(d[col], errors="coerce")
        elif to in ("date", "datetime", "timestamp"):
            d[col] = pd.to_datetime(d[col], errors="coerce")
        elif to in ("boolean", "bool"):
            d[col] = d[col].astype("boolean")
    return d


def _exec_SuperExtensibilityNode(
    df: pd.DataFrame, node: Node, ctx: Context
) -> pd.DataFrame:
    # Try to call provided hook; if missing, pass-through
    setup = (node.config.get("actionNode") or {}).get("setupParameters") or {}
    execp = (node.config.get("actionNode") or {}).get("executionParameters") or {}
    info = {"setup": setup, "exec": execp, "name": node.name}
    if ctx.extensibility_hook:
        try:
            return ctx.extensibility_hook(df.copy(), info)
        except Exception as e:
            ctx.warnings.append(
                f"Extensibility step '{node.name}' failed: {e}; passing through."
            )
            return df
    # No hook supplied: just pass-through
    ctx.warnings.append(f"Skipping external script '{node.name}' (no hook supplied).")
    return df


def _exec_WriteToHyper(df: pd.DataFrame, node: Node, ctx: Context) -> pd.DataFrame:
    # Write CSV instead of Hyper
    fname = re.sub(r"[^A-Za-z0-9._-]+", "_", node.name) + ".csv"
    path = os.path.join(ctx.outdir, fname)
    df.to_csv(path, index=False)
    print(f"[Output] {node.name} -> {path}")
    return df


def _exec_SuperUnion(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    frames = [d.copy() for d in dfs if isinstance(d, pd.DataFrame)]
    if not frames:
        return pd.DataFrame()

    # Standardize column names across different survey years
    # Map new column names to old/standard names for consistency
    column_mapping = {
        # Daniels School -> Krannert School (standardize on Krannert)
        "Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Mitch Daniels School of Business.": "Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School",
        # Graduate Business Program -> MBA/MS Program
        "What were the deciding factors you used when choosing your future Graduate Business Program? (Please Select all that Apply): - Selected Choice": "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -",
        "What were the deciding factors you used when choosing your future Graduate Business Program? (Please Select all that Apply): - Other - Text": "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): - Other - Text",
        # Daniels School program names -> Krannert
        "Program at the Daniels School of Business to which you applied (select all that apply): - Selected Choice": "Program at Krannert to which you applied: -  Selected Choice",
        "Program at the Daniels School of Business to which you applied (select all that apply): - Other - Text": "Program at Krannert to which you applied: - Other - Text",
        # MBA/MS/Online variations
        "Did you decide to choose an MBA/MS or other Graduate Program at a different school?": "Did you decide to choose an MBA/MS/Online or other Graduate Program at a different school?",
        # School/Program names
        "Name of the school that you have chosen to enroll with:": "Name of the school that you will be attending:",
        # Program type descriptions
        'Please select the response that best describes the type of program you enrolled in. Or you can identify the specific program by selecting "Other" and entering the program in: - Selected Choice': "Please select the response that best describes the type of program you enrolled in. Or you can identify the specific program  1",
        'Please select the response that best describes the type of program you enrolled in. Or you can identify the specific program by selecting "Other" and entering the program in: - Other (Please specify) - Text': "Please select the response that best describes the type of program you enrolled in. Or you can identify the specific program  1 - Other - Text",
        # Other schools
        "To which other schools did you apply? Please do NOT use acronyms. (If applicable)": "To which other schools did you apply? (If applicable)",
        # Gender field
        "How do you describe yourself? - Selected Choice": "Gender",
    }

    standardized_frames = []
    for df in frames:
        df_copy = df.copy()

        # Rename columns that have mappings
        rename_dict = {
            old: new for old, new in column_mapping.items() if old in df_copy.columns
        }

        if rename_dict:
            df_copy = df_copy.rename(columns=rename_dict)
        standardized_frames.append(df_copy)

    return pd.concat(standardized_frames, ignore_index=True, sort=False)


def _exec_Container(
    start_node_id: str, container: Dict[str, Any], ctx: Context
) -> pd.DataFrame:
    # Execute the loomContainer inner graph linearly from its initial node through nextNodes
    nodes_raw = container.get("nodes") or {}

    # build simple map
    def _wrap(nid: str, obj: Dict[str, Any]) -> Node:
        return Node(
            id=obj.get("id") or nid,
            name=obj.get("name") or nid,
            nodeType=obj.get("nodeType") or "Unknown",
            baseType=obj.get("baseType") or "transform",
            nextNodes=[
                (nn.get("nextNodeId"), nn.get("nextNamespace") or "Default")
                for nn in obj.get("nextNodes", [])
            ],
            config=obj,
        )

    inner_nodes = {nid: _wrap(nid, obj) for nid, obj in nodes_raw.items()}
    cur = inner_nodes.get(start_node_id)
    if cur is None:
        return pd.DataFrame()
    # We assume a linear chain in this container (common for Clean sequences)
    df = None
    while True:
        if cur.nodeType == ".v2019_1_4.Remap":
            df = _exec_Remap(df, cur)
        elif cur.nodeType == ".v2019_1_4.ChangeType":
            df = _exec_ChangeType(df, cur)
        elif cur.baseType == "transform" and df is None:
            # First transform in container receives from outside; we'll just carry through
            pass
        # move next
        if not cur.nextNodes:
            break
        nxt_id, _ = cur.nextNodes[0]
        cur = inner_nodes.get(nxt_id)
        if cur is None:
            break
    return df


def run_flow(
    flow_path: str,
    outdir: str = "./prep_outputs",
    filename_map: Optional[Dict[str, str]] = None,
    extensibility_hook: Optional[
        Callable[[pd.DataFrame, Dict[str, Any]], pd.DataFrame]
    ] = None,
) -> Dict[str, pd.DataFrame]:
    fj = load_flow(flow_path)
    fl = parse(fj)
    ctx = Context(flow_path, outdir, filename_map, extensibility_hook)

    # Build reverse edges for execution: we will do a straightforward forward traversal from initial nodes.
    # Cache will store outputs per node id.
    # For this tailored flow, edges form a mostly linear graph with unions.

    # Helper to fetch upstream dataframes for a node (by scanning who points to it)
    rev_children = {}
    for nid, n in fl.nodes.items():
        for cid, _ns in n.nextNodes:
            rev_children.setdefault(cid, []).append(nid)

    # Simple DFS from each initial node
    def eval_node(nid: str) -> pd.DataFrame:
        if nid in ctx.cache:
            return ctx.cache[nid]
        node = fl.nodes[nid]
        # gather upstream outputs
        upstream_ids = rev_children.get(nid, [])
        upstream = [eval_node(uid) for uid in upstream_ids]

        # dispatch
        if node.nodeType == ".v2022_4_1.LoadExcel":
            out = _exec_LoadExcel(node, fl, ctx)
        elif node.nodeType == ".v2018_2_3.SuperUnion":
            out = _exec_SuperUnion(upstream)
        elif node.nodeType == ".v2019_2_2.SuperExtensibilityNode":
            # apply annotations first if present (Remap/ChangeType listed in config['annotations'])
            df_in = upstream[0] if upstream else pd.DataFrame()
            ann = (node.config.get("annotations") or {}).get("annotations") or []
            # Each annotation has a node under 'annotationNode'
            for a in ann:
                a_node = a.get("annotationNode") or {}
                a_wrap = Node(
                    id=a_node.get("id", ""),
                    name=a_node.get("name", ""),
                    nodeType=a_node.get("nodeType", ""),
                    baseType=a_node.get("baseType", "transform"),
                    nextNodes=[],
                    config=a_node,
                )
                if a_wrap.nodeType == ".v2019_1_4.Remap":
                    df_in = _exec_Remap(df_in, a_wrap)
                elif a_wrap.nodeType == ".v2019_1_4.ChangeType":
                    df_in = _exec_ChangeType(df_in, a_wrap)
            out = _exec_SuperExtensibilityNode(df_in, node, ctx)
        elif node.nodeType == ".v1.Container":
            # Take upstream[0] into container, run inner nodes (we handle only Remap/ChangeType pipelines)
            df_in = upstream[0] if upstream else pd.DataFrame()
            start = (
                (node.config.get("loomContainer") or {}).get("initialNodes") or [None]
            )[0]
            # Apply container ops to df_in by temporarily setting as current df
            # Our _exec_Container currently doesn't consume df_in internally;
            # to keep behavior, we'll run each inner op against df_in iteratively.
            inner = node.config.get("loomContainer") or {}
            nodes_raw = inner.get("nodes") or {}
            # Build linear order from initial
            cur_id = start
            out = df_in.copy()
            while cur_id and cur_id in nodes_raw:
                obj = nodes_raw[cur_id]
                cur_type = obj.get("nodeType")
                wrap = Node(
                    id=obj.get("id", ""),
                    name=obj.get("name", ""),
                    nodeType=cur_type,
                    baseType=obj.get("baseType", "transform"),
                    nextNodes=[
                        (nn.get("nextNodeId"), nn.get("nextNamespace") or "Default")
                        for nn in obj.get("nextNodes", [])
                    ],
                    config=obj,
                )
                if cur_type == ".v2019_1_4.Remap":
                    out = _exec_Remap(out, wrap)
                elif cur_type == ".v2019_1_4.ChangeType":
                    out = _exec_ChangeType(out, wrap)
                # advance
                cur_id = wrap.nextNodes[0][0] if wrap.nextNodes else None
        elif node.nodeType == ".v1.WriteToHyper":
            df_in = upstream[0] if upstream else pd.DataFrame()
            out = _exec_WriteToHyper(df_in, node, ctx)
        else:
            # passthrough for unknown transforms: carry first upstream
            out = upstream[0] if upstream else pd.DataFrame()

        ctx.cache[nid] = out
        # propagate to children (evaluate to ensure outputs written)
        for cid, _ns in node.nextNodes:
            if cid in fl.nodes:
                eval_node(cid)
        return out

    # Kick off evaluation
    for root in fl.initial:
        if root in fl.nodes:
            eval_node(root)

    return ctx.cache
