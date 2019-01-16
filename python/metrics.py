# import functools
# from joblib import Memory

from pyspark.sql import DataFrame

# memory = Memory(".", verbose=0)


class QII(object):
    def __init__(self, dataset: DataFrame, model, dist_in, dist_out):
        self.dataset = dataset
        self.model = model
        self.dist_in = dist_in
        self.dist_out = dist_out

    # @functools.lru_cache(maxsize=None)
    # @memory.cache
    def single(self, row, factor):

        output = self.model.predict([row])[0]

        diff = 0.0
        count = 0

        for row_ in self.dataset.iterrows():
            cf_row = row.copy()

            temp_row = row_[1]

            count += 1

            cf_row[factor] = temp_row[factor]
            cf_output = self.model.predict([cf_row])[0]

            if output.__class__ != cf_output.__class__:
                raise Exception("counterfactual is of different class than original: " +
                                f"{output.__class__} != {cf_output.__class__}")

            if self.dist_in(row, cf_row) == 0.0 and self.dist_out(cf_output, output) > 0.0:
                raise Exception("input distance is zero but output distance is nonzero")
            else:
                diff += self.dist_out(cf_output, output) / self.dist_in(row, cf_row)

        return diff / float(count)
