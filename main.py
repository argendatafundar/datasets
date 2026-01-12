from argendata_datasets.analyze.comparar_df import comparar_df

import polars as pl
import matplotlib.pyplot as plt

# assume you put the functions in compare_outputs.py
# from compare_outputs import comparar_df

# --- previous output ---
df_prev = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "monto": [10.0, 12.0, 11.5, 9.0, 10.0],
        "estado": ["A", "A", "B", "B", "C"],
    }
)

# --- new output ---
df_new = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        # introduce a big change (outlier-ish) and a new null
        "monto": [10.1, None, 11.6, 30.0, 10.2],
        # introduce a mismatch
        "estado": ["A", "A", "B", "X", "C"],
        # introduce a new column
        "fuente": ["x", "x", "y", "y", "z"],
    }
)

res = comparar_df(
    df=df_new,
    df_anterior=df_prev,
    pk=["id"],
    k_control_num=3,
    drop_joined_df=False,
    make_plots=True,
)

# High-level outputs
print("diferencia_nfilas:", res["diferencia_nfilas"])
print("check_columnas:", res["check_columnas"])
print("comparacion_clases:\n", res["comparacion_clases"])

# Per-column checks
monto_chk = res["comparacion_cols"]["monto"]
print("\n[monto] nuevos_na:", monto_chk["nuevos_na"])
print("[monto] mean_variaciones_rel:", monto_chk["mean_variaciones_rel"])
print("[monto] ks_test pvalue:", monto_chk["ks_test"])
print("[monto] mw_test pvalue:", monto_chk["mw_test"])
print("[monto] tasa_posibles_outliers:", monto_chk["tasa_posibles_outliers"])
print("[monto] filas_posibles_outliers:\n", monto_chk["filas_posibles_outliers"])

estado_chk = res["comparacion_cols"]["estado"]
print("\n[estado] nuevos_na:", estado_chk["nuevos_na"])
print("[estado] tasa_mismatches:", estado_chk["tasa_mismatches"])
print("[estado] filas_mismatches:\n", estado_chk["filas_mismatches"])

# Plot (matplotlib Figure) for numeric columns
fig = monto_chk["plot"]
if fig is not None:
    fig.savefig("monto_compare.png", dpi=150, bbox_inches="tight")
    plt.show()

# Joined DF is included unless drop_joined_df=True
print("\njoined_df:\n", res["joined_df"])
