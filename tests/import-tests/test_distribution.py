import pandas as pd
from utils import gini_from_counts, cv_from_counts

def test_imported_data_distribution_light(
    df: pd.DataFrame,
):
    date_col: str = "created_date",  
    date_col = "created_date"
    max_gini = 0.55
    max_cv = 0.55
    min_days_covered = 20
    also_check_weekly = True

    assert date_col in df.columns, f"Column '{date_col}' not in df"

    problems = []

    for month, g in df.groupby("create_month"):
        # Counts pro Tag
        daily = g["create_day"].value_counts().sort_index()

        # Abdeckung: wie viele unterschiedliche Tage mit Daten?
        days_covered = int((daily > 0).sum())

        # Metriken
        gini = gini_from_counts(daily)
        cv = cv_from_counts(daily)

        # Regeln prüfen
        msgs = []
        if days_covered < min_days_covered:
            msgs.append(f"low day coverage ({days_covered} < {min_days_covered})")
        if gini > max_gini:
            msgs.append(f"high Gini ({gini:.3f} > {max_gini:.2f})")
        if cv > max_cv:
            msgs.append(f"high CV ({cv:.3f} > {max_cv:.2f})")

        # Optional: Wochen-Check (aggregierter Blick)
        if also_check_weekly:
            wk = g["created_date"].dt.isocalendar().week
            weekly = wk.value_counts().sort_index()
            cv_w = cv_from_counts(weekly)
            # Wochen dürfen etwas variieren; oft strenger als täglich
            if cv_w > max(max_cv, 0.45):
                msgs.append(f"weekly CV high ({cv_w:.3f} > {max(max_cv, 0.45):.2f})")

        if msgs:
            problems.append(f"{month}: {', '.join(msgs)}")

    assert  len(problems) <= 6, "Non-uniform monthly sampling:\n  - " + "\n  - ".join(problems)