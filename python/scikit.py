"""Scikit utilities."""

import numpy as np


def tree_used_features(tree):
    """Get indices of the features used by a given decision tree."""

    left = tree.tree_.children_left
    right = tree.tree_.children_right
    features = tree.tree_.feature

    ret = set()

    def recurse(left, right, node):
        """Up the tree you go."""

        ret.add(features[node])
        if left[node] != -1:
            recurse(left, right, left[node])
        if right[node] != -1:
            recurse(left, right, right[node])

    recurse(left, right, 0)

    return list(ret)


def get_code(tree, feature_names, target_names,
             spacer_base="    "):
    """Produce psuedo-code for decision tree.

    Args
    ----
    tree -- scikit-leant DescisionTree.
    feature_names -- list of feature names.
    target_names -- list of target (class) names.
    spacer_base -- used for spacing code (default: "    ").

    Notes
    -----
    based on http://stackoverflow.com/a/30104792.
    """

    left = tree.tree_.children_left
    right = tree.tree_.children_right
    threshold = tree.tree_.threshold
    features = [feature_names[i] for i in tree.tree_.feature]
    value = tree.tree_.value

    def recurse(children, threshold, features, node, depth):
        """Up the tree you go."""

        (left, right) = children

        spacer = spacer_base * depth
        if threshold[node] != -2:
            print(spacer + "if ( " + features[node] + " <= " +
                  str(threshold[node]) + " ) {")
            if left[node] != -1:
                recurse((left, right), threshold, features,
                        left[node], depth+1)
            print(spacer + "}\n" + spacer + "else {")
            if right[node] != -1:
                recurse((left, right), threshold, features,
                        right[node], depth+1)
            print(spacer + "}")
        else:
            target = value[node]
            for idx, val in zip(np.nonzero(target)[1],
                                target[np.nonzero(target)]):
                target_name = target_names[idx]
                target_count = int(val)
                print(spacer + "return " + str(target_name) +
                      " ( " + str(target_count) + " examples )")

    recurse((left, right), threshold, features, 0, 0)
