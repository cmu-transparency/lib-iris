"""Apache Spark utilities."""

import os
import shutil
from typing import List, Iterable

from pyspark import SparkContext, SparkConf      # pylint: disable=import-error
from pyspark.sql import SQLContext, SparkSession, DataFrame  # pylint: disable=import-error
from pyspark.sql.types import StringType         # pylint: disable=import-error
from pyspark.ml.feature import StringIndexer, VectorAssembler  # pylint: disable=import-error

from .misc import named_of_indexed, tab, A
from .jvm import class_name


# SparkContext.setSystemProperty('spark.executor.memory', '8g')
# SparkContext.setSystemProperty('spark.python.worker.memory', '8g')
# SparkContext.setSystemProperty('spark.driver.memory', '16g')
# ss = SparkSession\
#      .builder\
#      .master("local[20]")\
#      .appName("testing")\
#      .config('spark.executor.memory', '8G')\
#      .config('spark.worker.memory', '8G')\
#      .config('spark.driver.memory', '8G')\
#      .getOrCreate()
#      .set('spark.driver.memory', '45G')
#      .set('spark.driver.maxResultSize', '10G')
# conf = SparkConf().setAppName("App")
# conf = (conf.setMaster('local[*]')
#         .set('spark.executor.memory', '4G')
#         .set('spark.driver.memory', '45G')
#         .set('spark.driver.maxResultSize', '10G'))

# old spark utils #

class ByKeyAndIndex(object):  # pylint: disable=too-few-public-methods
    """A list of items that can be accessed either by index or by a key."""

    __slots__ = ['keys_by_index', 'items_by_index', 'items_by_key']

    def __init__(self, items_by_index, keys=None):
        """If the keys for the items are not provided, the items themselves become the keys."""

        self.keys_by_index = keys
        self.items_by_index = items_by_index
        self.items_by_key = named_of_indexed(self.items_by_index, keys=keys)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.items_by_index[index]
        return self.items_by_key[str(index)]


class ValueMapper(object):  # pylint: disable=too-many-instance-attributes
    """Holds the information about column names of a dataset, and the values of a dataset that were
    converted to doubles in order to use various spark models.
    """

    __slots__ = ['columns', 'types', 'values', 'indices', "target",
                 "columns_by_idx", "types_by_idx", "values_by_idx", "target_name"]

    def __init__(self, columns_by_idx: List[str], types_by_idx, values_by_idx):
        # self.target_idx = target_idx
        # self.target_name = str(columns_by_idx[target_idx])
        self.columns_by_idx = columns_by_idx
        self.types_by_idx = types_by_idx
        self.values_by_idx = values_by_idx
        self.target = None
        self.target_name = None

        self._reconstruct()

    def _reconstruct(self):
        self.columns = ByKeyAndIndex(self.columns_by_idx, keys=self.columns_by_idx)
        self.types = ByKeyAndIndex(self.types_by_idx, keys=self.columns_by_idx)
        self.values = ByKeyAndIndex(self.values_by_idx, keys=self.columns_by_idx)
        self.indices = ByKeyAndIndex(self._compute_indices(), keys=self.columns_by_idx)

    def remove(self, idx: int):
        """Remove an item by index."""

        self.columns_by_idx.pop(idx)
        self.types_by_idx.pop(idx)
        self.values_by_idx.pop(idx)
        self._reconstruct()

    def _compute_indices(self):
        return [{name: index for (name, index) in zip(col, range(len(col)))}
                for col in self.values]

    def set_target(self, target_col: str):
        """Set target column name."""

        target_idx = self.columns.keys_by_index.index(target_col)
        target_type = self.types[target_idx]
        target_values = self.values[target_idx]
        self.target_name = target_col

        print(target_idx)
        print(target_type)
        print(target_values)

        self.target = ValueMapper([target_col], [target_type], [target_values])
        self.remove(target_idx)


class Spark(object):
    """Store the various elements of of a spark setup and context."""

    __slots__ = ['conf', 'spark', 'sql', "session", "jvm", "master"]

    def __init__(self, master="local[*]") -> None:
        """Create only the spark configuration at first."""

        self.conf = SparkConf().setMaster(master)  # pylint: disable=undefined-variable
        self.master = master

        self.spark = None
        self.sql = None

        self.jvm = None
        self.session = None

    def start(self):
        """Initialize the rest of the stuff, assuming the configuration has been filled in."""

        self.session = SparkSession\
            .builder\
            .master(self.master)\
            .appName("testing")\
            .getOrCreate()
        self.spark = self.session.sparkContext
        self.sql = SQLContext(self.spark)
        self.jvm = SparkContext._jvm  # pylint: disable=protected-access

        print("spark version = " + self.spark.version)

        for row in self.spark._conf.getAll():  # pylint: disable=protected-access
            print(row)

        return self

    def __getattr__(self, name):
        """For attributes not provided by this Spark object, try to find the attribute in one of
        the three objects from spark itself.
        """

        component = None

        if hasattr(self.conf, name):
            component = "conf"
        elif hasattr(self.spark, name):
            component = "spark"
        elif hasattr(self.sql, name):
            component = "sql"
        else:
            raise ValueError("no such attribute: %s" % name)

        subobject = getattr(self, component)
        field = getattr(subobject, name)

        def enclosed(*args, **kwargs):
            """If the field is a method, and it returns one of the 3 subcomponents, retrofit it so
            that it returns this whole class after updating the subcomponent.
            """

            ret = field(*args, **kwargs)
            if type(ret) is type(subobject):
                setattr(self, component, ret)
                return self
            return ret

        if callable(field):
            return enclosed
        return field

    def load_csv(self, filename, sep=","):
        """Load a CSV file into a dataframe."""

        return self.session.read.csv(
            filename,
            inferSchema=True,
            sep=sep,
            header=True
        )

    def save_csv(self, filename: str, dataframe: DataFrame, sep: str = ","):
        # pylint: disable=no-self-use
        """Save a dataframe to CSV file."""

        if os.path.exists(filename):
            shutil.rmtree(filename)

        return dataframe.write.csv(
            filename,
            sep=sep,
            header=True
        )

    def read_csv(self,
                 filename,
                 header='true',
                 encoding='UTF-8',
                 inferschema='true'):
        """Read a CSV file into a dataframe."""

        return self.sql.read.format("com.databricks.spark.csv").options(
            header=header,
            encoding=encoding,
            inferschema=inferschema
        ).load(filename)


def assemble_features(dataframe,
                      columns,
                      output_column="features",
                      exclude=None):
    """Collect the set of given features into a vector column to use for training."""

    if exclude is None:
        exclude = []

    assembler = VectorAssembler(inputCols=[col for col in columns
                                           if col not in exclude],
                                outputCol=output_column)

    return assembler.transform(dataframe)


def index_nominals(dataframe,
                   renamer=lambda string: u"indexed_%s" % string):
    """Create indexed versions of nominal features in the given dataframe."""

    all_cols = dataframe.columns

    schema = dataframe.schema
    names_by_idx = [str(name) for name in schema.names]
    types_by_idx = [field.dataType for field in schema.fields]
    labels_by_idx = [[] for idx in range(len(all_cols))]

    dataframe_indexing = dataframe

    # The new (or old if not indexed) column names.
    columns = []

    # Fit and apply a sequence of nominal feature indexer.
    for idx, col in enumerate(all_cols):
        if types_by_idx[idx] is StringType():
            # Encode nominal features into doubles.
            indexer = StringIndexer(
                inputCol=col,
                outputCol=renamer(col)
            ).fit(dataframe_indexing)

            labels_by_idx[idx] = indexer.labels
            dataframe_indexing = indexer.transform(dataframe_indexing)
            columns.append(renamer(col))
        else:
            labels_by_idx[idx] = []
            columns.append(col)

    # Create the object that holds the information necessary to get
    # column and value names for the various converted features and
    # values.
    namer = ValueMapper(
        columns_by_idx=names_by_idx,
        types_by_idx=types_by_idx,
        values_by_idx=labels_by_idx
    )

    return dataframe_indexing, columns, namer


def pylist_of_scala_list(alist: Iterable[A]):
    """Convert scala type :: (the cons in a lisp-like language) to a python list. Py4j has issues
    with these for some reason.
    """

    typ = class_name(alist)

    if typ == "Nil$":
        return []
    elif typ == "$colon$colon":
        return [alist.head()] + pylist_of_scala_list(alist.tail())


def string_of_mllib_split(split, namer=None):
    """String representation of a scala spark mllib (old API) Split ."""

    typ = class_name(split)

    if typ != "Split":
        raise ValueError("unknown class: %s" % typ)

    ret = u""
    if namer is None:
        ret = u"f%d " % split.feature()
    else:
        ret = u"%s " % namer.columns[split.feature()]

    stype = split.featureType().toString()

    if stype == "Categorical":
        cats = pylist_of_scala_list(split.categories())
        ret += u"∈ {%s}" % u",".join([str(c) for c in cats])
    elif stype == "Continuous":
        ret += u"≤ %f" % split.threshold()

    return ret


def string_of_mllib_node(node, namer=None):
    """String representation of a scala spark mllib (old API) Node."""

    typ = class_name(node)

    if typ != "Node":
        raise ValueError("unknown class: %s" % typ)

    predict = node.predict().predict()

    info = u""
    if namer is not None:
        info = u"prediction: %s = %s" % (namer.target_name, str(predict))
    else:
        info = u"prediction: output = %s" % str(predict)

    ret = u""

    if not node.isLeaf():
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.InternalNode
        ret += u"if %s (%s) then\n" % \
               (string_of_mllib_split(
                  node.split().get(),
                  namer),
                info) + \
               tab(string_of_mllib_node(node.leftNode().get(), namer)) + u"\n" + \
               u"else\n" + \
               tab(string_of_mllib_node(node.rightNode().get(), namer))

    else:
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.LeafNode
        if namer is None:
            ret += u"output = %s" % str(predict)
        else:
            ret += u"%s = %s" % (namer.target_name,
                                 namer.values
                                 [namer.target_idx]
                                 [predict])

    return ret


def string_of_split(split, namer=None):
    """String representation of a scala spark Split:
    https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.Split
    """

    typ = class_name(split)

    ret = u""
    if namer is None:
        ret = u"f%d " % split.featureIndex()
    else:
        ret = u"%s " % namer.columns[split.featureIndex()]

    idx = split.featureIndex()

    if typ == "CategoricalSplit":
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.CategoricalSplit

        if namer is None:
            ret += "∈ {%s}" % u",".join([str(c) for c in split.leftCategories()])
        else:
            ret += "∈ {%s}" % u",".join([namer.values[idx][int(cat)]
                                         for cat in split.leftCategories()])
    elif typ == "ContinuousSplit":
        ret += u"≤ %f" % split.threshold()

    else:
        ret += u"unknown split: %s (%s)" % (str(split), str(typ))

    return ret


def string_of_node(node, namer=None):
    """String representation of a scala spark Node:
    https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.Node
    """

    typ = class_name(node)

    info = u""

    if namer is None:
        info = u"prediction: output = %d" % node.prediction()
    else:
        info = u"prediction: %s = %s" % (namer.target_name,
                                         namer.target.values
                                         [0]
                                         [int(node.prediction())])

    ret = u""

    if typ == "InternalNode":
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.InternalNode
        ret += u"if %s (%s) then\n" % (string_of_split(node.split(), namer),
                                       info) + \
               tab(string_of_node(node.leftChild(), namer)) + u"\n" + \
               u"else\n" + \
               tab(string_of_node(node.rightChild(), namer))

    elif typ == "LeafNode":
        # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.tree.LeafNode
        if namer is None:
            ret += u"output = %d" % int(node.prediction())
        else:
            ret += u"%s = %s" % (namer.target_name,
                                 namer.target.values
                                 [0]
                                 [int(node.prediction())])
    else:
        ret += u"unknown class"

    return ret
