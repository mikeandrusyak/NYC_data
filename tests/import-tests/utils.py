import numpy as np
import pandas as pd

def gini_from_counts(counts: pd.Series) -> float:
    """Gini-Koeffizient der Ungleichverteilung, 0 = perfekt gleich."""
    x = counts.to_numpy(dtype=float)
    if x.size == 0 or x.sum() == 0:
        return 0.0
    x_sorted = np.sort(x)
    n = x_sorted.size
    return (2 * np.sum((np.arange(1, n + 1) * x_sorted)) / (n * x_sorted.sum())) - (n + 1) / n

def cv_from_counts(counts: pd.Series) -> float:
    """Variationskoeffizient = std/mean. Robuster als p-Wert bei großen n."""
    x = counts.to_numpy(dtype=float)
    m = x.mean()
    if m == 0:
        return 0.0
    return float(x.std(ddof=0) / m)