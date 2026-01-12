"""
Traducido automáticamente desde

https://github.com/argendatafundar/argendataR/blob/master/R/comparar_df.R
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import warnings

import numpy as np
import polars as pl
from scipy import stats

# Optional plotting (kept lightweight, matplotlib only)
import matplotlib.pyplot as plt


def _ensure_pk(pk: Optional[Sequence[str]]) -> List[str]:
    if pk is None:
        raise ValueError("parametro 'pk' debe ser provisto (no puede ser None)")
    if not isinstance(pk, (list, tuple)) or not all(isinstance(x, str) for x in pk):
        raise TypeError("parametro 'pk' debe ser una lista/tupla de strings")
    return list(pk)


def _to_float_np(s: pl.Series) -> np.ndarray:
    """
    Convert a polars Series to float numpy array.
    - Nulls become np.nan
    - Non-castable values become np.nan
    """
    # strict=False turns uncastable values into null; then numpy -> nan
    return s.cast(pl.Float64, strict=False).to_numpy()


def nuevos_na(x: pl.Series, y: pl.Series) -> int:
    """
    Cantidad de NA/Null nuevos en y respecto de x.
    (equivalente a sum(!is.na(x) & is.na(y)) )
    """
    if len(x) != len(y):
        raise ValueError("los vectores deben tener igual largo")
    return int(((~x.is_null()) & (y.is_null())).sum())


def check_cols(df: pl.DataFrame, df_anterior: pl.DataFrame) -> Dict[str, List[str]]:
    cols_previas = list(df_anterior.columns)
    cols_actuales = set(df.columns)

    cols_faltantes = [c for c in cols_previas if c not in cols_actuales]
    if cols_faltantes:
        warnings.warn(
            "Columnas faltantes:\n" + ", ".join(cols_faltantes),
            stacklevel=2,
        )

    cols_nuevas = [c for c in df.columns if c not in set(cols_previas)]
    if cols_nuevas:
        warnings.warn(
            "Columnas nuevas:\n" + ", ".join(cols_nuevas),
            stacklevel=2,
        )

    return {"cols_nuevas": cols_nuevas, "cols_faltantes": cols_faltantes}


def check_datatypes(df: pl.DataFrame, df_anterior: pl.DataFrame) -> pl.DataFrame:
    """
    Compara dtypes (Polars) entre el output previo y el nuevo.
    """
    prev = pl.DataFrame(
        {
            "columna": list(df_anterior.schema.keys()),
            "clase_previa": [str(t) for t in df_anterior.schema.values()],
        }
    )
    new = pl.DataFrame(
        {
            "columna": list(df.schema.keys()),
            "clase_nueva": [str(t) for t in df.schema.values()],
        }
    )

    df_clases = (
        prev.join(new, on="columna", how="left")
        .filter(pl.col("clase_nueva").is_not_null())
        .with_columns((pl.col("clase_previa") == pl.col("clase_nueva")).alias("coinciden"))
    )

    mism = df_clases.filter(~pl.col("coinciden"))
    if mism.height > 0:
        warnings.warn(
            "Mismatch de clases:\n" + repr(mism.to_dicts()),
            stacklevel=2,
        )

    return df_clases


def _make_joined_df(df: pl.DataFrame, df_anterior: pl.DataFrame, pk: List[str]) -> pl.DataFrame:
    """
    Reproduce (aprox.) el comportamiento de dplyr::left_join(df_anterior, df, by=pk):
    - columnas solapadas (no PK) -> .x y .y
    - columnas solo del anterior -> quedan igual
    - columnas solo del nuevo -> quedan igual
    Polars left join preserva el orden del left DF. :contentReference[oaicite:2]{index=2}
    """
    overlap = [c for c in df_anterior.columns if c in set(df.columns) and c not in set(pk)]
    left = df_anterior.rename({c: f"{c}.x" for c in overlap})
    right = df.rename({c: f"{c}.y" for c in overlap})
    return left.join(right, on=pk, how="left")


def _is_numeric_dtype(dtype: pl.DataType) -> bool:
    # Polars supports numeric types including integers, floats, decimals :contentReference[oaicite:3]{index=3}
    # We treat any dtype that reports is_numeric() as numeric (covers ints/floats/decimals).
    try:
        return bool(dtype.is_numeric())
    except Exception:
        # Fallback for older versions: match common numeric base types
        return dtype in {
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64,
        }


def control_valores_nonnum(root_name: str, pk: Sequence[str], df: pl.DataFrame) -> Dict[str, Any]:
    col_x = f"{root_name}.x"
    col_y = f"{root_name}.y"

    if col_x not in df.columns or col_y not in df.columns:
        raise KeyError(f"Faltan columnas esperadas: {col_x} / {col_y}")

    na_count = nuevos_na(df[col_x], df[col_y])

    work = df.with_columns(
        (
            (pl.col(col_y) == pl.col(col_x)).fill_null(False)
            | (pl.col(col_y).is_null() & pl.col(col_x).is_null())
        ).alias("coinciden_valores")
    )

    mismatches_df = work.filter(~pl.col("coinciden_valores")).select(
        list(pk) + [col_x, col_y, "coinciden_valores"]
    )

    coinciden = work["coinciden_valores"].to_numpy()
    tasa_mismatches = float(np.mean(~coinciden)) if len(coinciden) else 0.0

    return {
        "nuevos_na": na_count,
        "tasa_mismatches": tasa_mismatches,
        "filas_mismatches": mismatches_df,
    }


def control_valores_num(
    root_name: str,
    pk: Sequence[str],
    k: float,
    df: pl.DataFrame,
    make_plot: bool = True,
) -> Dict[str, Any]:
    col_x = f"{root_name}.x"
    col_y = f"{root_name}.y"

    if col_x not in df.columns or col_y not in df.columns:
        raise KeyError(f"Faltan columnas esperadas: {col_x} / {col_y}")

    na_count = nuevos_na(df[col_x], df[col_y])

    x = _to_float_np(df[col_x])
    y = _to_float_np(df[col_y])

    # variaciones_rel = (y - x) / x  (como en R; si x==0 => inf)
    with np.errstate(divide="ignore", invalid="ignore"):
        variaciones_rel = np.round((y - x) / x, 6)

    abs_variaciones = np.abs(variaciones_rel)
    finite_mask = np.isfinite(abs_variaciones)
    mean_variaciones_rel = float(np.nanmean(abs_variaciones[finite_mask])) if finite_mask.any() else float("nan")

    # zscore sobre variaciones_rel, ignorando nan/inf
    vr = variaciones_rel.copy()
    vr[~np.isfinite(vr)] = np.nan
    mu = np.nanmean(vr)
    sigma = np.nanstd(vr, ddof=0)
    if np.isfinite(sigma) and sigma > 0:
        z = (vr - mu) / sigma
    else:
        z = np.full_like(vr, np.nan)

    work = df.with_columns(
        [
            pl.Series("variaciones_rel", variaciones_rel),
            pl.Series("zscaled_variaciones_rel", z),
            pl.concat_str(
                [pl.col(c).cast(pl.Utf8) for c in pk],
                separator=" - ",
            ).alias("label"),
        ]
    )

    # subset for tests: non-null x/y and finite numbers
    test_df = work.filter(pl.col(col_x).is_not_null() & pl.col(col_y).is_not_null())
    x2 = _to_float_np(test_df[col_x])
    y2 = _to_float_np(test_df[col_y])
    mask2 = np.isfinite(x2) & np.isfinite(y2)

    if mask2.sum() >= 2:
        # KS 2-sample (two-sided default) :contentReference[oaicite:4]{index=4}
        ks_p = float(stats.ks_2samp(x2[mask2], y2[mask2]).pvalue)
        # Mann–Whitney U (two-sided default alternative) :contentReference[oaicite:5]{index=5}
        mw_p = float(stats.mannwhitneyu(x2[mask2], y2[mask2], alternative="two-sided", method="auto").pvalue)
    else:
        ks_p = float("nan")
        mw_p = float("nan")

    # outliers: abs(z) > k and finite
    outliers_df = work.filter(
        pl.col("zscaled_variaciones_rel").is_finite()
        & (pl.col("zscaled_variaciones_rel").abs() > float(k))
    ).select(list(pk) + [col_x, col_y, "variaciones_rel", "zscaled_variaciones_rel", "label"])

    tasa_posibles_outliers = float(outliers_df.height / work.height) if work.height else 0.0

    filas_nuevos_na = work.filter(pl.col(col_x).is_not_null() & pl.col(col_y).is_null())

    fig = None
    if make_plot:
        # scatter + OLS line + y=x; annotate only outliers to keep it readable
        fig, ax = plt.subplots(figsize=(6, 4))
        xx = _to_float_np(work[col_x])
        yy = _to_float_np(work[col_y])
        ok = np.isfinite(xx) & np.isfinite(yy)

        ax.scatter(xx[ok], yy[ok], s=12, facecolors="none", edgecolors="C0", alpha=0.8)

        if ok.sum() >= 2:
            m, b = np.polyfit(xx[ok], yy[ok], 1)
            xs = np.linspace(np.nanmin(xx[ok]), np.nanmax(xx[ok]), 100)
            ax.plot(xs, m * xs + b, color="C0", alpha=0.7)

            # y=x
            ax.plot(xs, xs, color="red", alpha=0.7)

        # annotate outliers
        if outliers_df.height > 0:
            od = outliers_df.select([col_x, col_y, "label"]).to_dicts()
            for row in od:
                ox = row[col_x]
                oy = row[col_y]
                lab = row["label"]
                if ox is not None and oy is not None:
                    ax.annotate(str(lab), (float(ox), float(oy)), fontsize=8, alpha=0.9)

        ax.set_xlabel(col_x)
        ax.set_ylabel(col_y)
        ax.set_title(root_name)
        ax.grid(True, alpha=0.2)
        fig.tight_layout()

    return {
        "nuevos_na": na_count,
        "mean_variaciones_rel": mean_variaciones_rel,
        "ks_test": ks_p,
        "mw_test": mw_p,
        "tasa_posibles_outliers": tasa_posibles_outliers,
        "plot": fig,
        "filas_nuevos_na": filas_nuevos_na,
        "filas_posibles_outliers": outliers_df,
    }


def comparar_df(
    df: pl.DataFrame,
    df_anterior: pl.DataFrame,
    pk: Optional[Sequence[str]] = None,
    k_control_num: float = 3.0,
    drop_joined_df: bool = False,
    make_plots: bool = True,
) -> Dict[str, Any]:
    """
    Traducción directa de comparar_df() (R) a Polars/SciPy.
    """
    pk_list = _ensure_pk(pk)

    if not (set(pk_list) <= set(df_anterior.columns) and set(pk_list) <= set(df.columns)):
        raise ValueError("las columnas 'pk' deben estar presentes en el output previo y el output nuevo")

    columns_check = check_cols(df=df, df_anterior=df_anterior)
    df_clases = check_datatypes(df=df, df_anterior=df_anterior)

    # cols to compare: in previous, not pk, and also present in new
    cols_comparacion = [
        c for c in df_anterior.columns
        if c not in set(pk_list) and c in set(df.columns)
    ]

    joined_df = _make_joined_df(df=df, df_anterior=df_anterior, pk=pk_list)

    comparacion_cols: Dict[str, Any] = {}
    for c in cols_comparacion:
        cx = f"{c}.x"
        cy = f"{c}.y"
        if cx not in joined_df.columns or cy not in joined_df.columns:
            # should not happen, but guard anyway
            comparacion_cols[c] = "Columnas esperadas no encontradas tras el join"
            continue

        dtype_x = joined_df.schema[cx]
        if _is_numeric_dtype(dtype_x):
            comparacion_cols[c] = control_valores_num(
                root_name=c,
                pk=pk_list,
                k=float(k_control_num),
                df=joined_df,
                make_plot=make_plots,
            )
        else:
            comparacion_cols[c] = control_valores_nonnum(
                root_name=c,
                pk=pk_list,
                df=joined_df,
            )

    resultado: Dict[str, Any] = {
        "check_columnas": columns_check,
        "comparacion_clases": df_clases,
        "diferencia_nfilas": int(df.height - df_anterior.height),
        "comparacion_cols": comparacion_cols,
    }

    if not drop_joined_df:
        resultado["joined_df"] = joined_df

    return resultado
