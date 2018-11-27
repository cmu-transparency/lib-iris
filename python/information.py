from . import misc

from misc import T

from typing import Iterable

from pyspark.sql import DataFrame

def entropy_of_counts(counts: Iterable[int]) -> float:
    total = 0
    for count in counts: total += count
    total = float(total)
    ret = 0.0
    for count in counts:
        p = float(count) / total
        ret -= p * misc.lg(p)
    return ret

def conditional_entropy_of_counts(counts: Dict[T, int]) -> float:
    totals = defaultdict(int)
    total = 0
    for (v, count) in counts:
        totals[v] += count
        total += count
    total = float(total)
    ret = 0.0

    for (v, count) in counts:
        t = float(totals[v])
        p1 = t/total
        p2 = float(count) / t
        ret -= p1 * p2 * misc.lg(p2)

    return ret

def _conditional_entropy(df: DataFrame, x: str, y: str) -> float:
    df_count = df.groupBy(x,y)
    df_count = df_count.agg({'*': 'count'})
    counts = list(map(lambda r: (r[y], r['count(1)']), df_count.collect()))
    return conditional_entropy_of_counts(counts)

def _entropy(df, x):
    df_count = df.groupBy(x).agg({'*': 'count'})
    counts = list(map(lambda r: r['count(1)'], df_count.collect()))
    #misc.printme("len(counts): " + str(len(counts)) + "\n")
    x_ent = entropy_of_counts(counts)
    return x_ent

def mutual_information(df, x, y, normalize=False):
    df = df.selectExpr(x, y)
    x_ent = _entropy(df, x)
    y_ent = _entropy(df, y)
    ent = min(x_ent, y_ent)
    x_cond_y_ent = _conditional_entropy(df, x, y)

    if normalize:
        if ent != 0.0: return (x_ent - x_cond_y_ent) / ent
        else: return 0.0
    else: return x_ent - x_cond_y_ent

def conditional_mutual_information(df, x, y, c):
