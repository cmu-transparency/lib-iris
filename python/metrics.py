import functools

from pyspark.sql import DataFrame


def qii(dataset: DataFrame):

    @functools.lru_cache(maxsize=None)
    def eval(user,
             model,
             request,
             request_factor,
             din,
             dout,
             **kwargs):

        # pred_index = dataset.columns.get_loc(pred_req)
        # test_index = dataset.columns.get_loc(test_req)
        a_output = model.recommend(user, request, **kwargs)

        diff = 0.0

        # dataset_np = np.array(dataset)
        # print(test_index)
        # print(dataset_np)
        # test_column = dataset_np[:,test_index]
        # print(dataset_np)
        # np.random.shuffle(test_column)
        # print(test_column)
        # dataset_np[:,test_index] = test_column
        # print(dataset_np)
        # test_preds = model.predict(dataset_np)

        count_diff_in = 0

        for row_ in dataset.iterrows():
            cf_user = user.copy()

            row = row_[1]

            if request_factor not in user.prefs:
                print(f"request\n\t{request_factor}\nnot in user.prefs\n\t{user.prefs}")
            if request_factor not in row:
                print(f"request\n\t{request_factor}\nnot in row\n\t{row}")

            if user.prefs[request_factor] == row[request_factor]:
                continue

            count_diff_in += 1

            cf_user.prefs[request_factor] = row[request_factor]

            cf_output = model.recommend(cf_user, request, **kwargs)

            if a_output.__class__ != cf_output.__class__:
                raise Exception("counterfactual is of different class than original: " +
                                f"{a_output.__class__} != {cf_output.__class__}")

            diff += dout(cf_output, a_output) / din(user, cf_user)

        if count_diff_in == 0:
            print(f"WARNING: factor {request_factor} has no deviation")
            return 0.0

        return diff / count_diff_in

    return eval
