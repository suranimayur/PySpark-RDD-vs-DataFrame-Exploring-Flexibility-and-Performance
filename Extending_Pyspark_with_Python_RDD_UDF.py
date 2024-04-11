'''
We will cover below topics as below

 Using the RDD as a low-level, flexible data container
 Manipulating data in the RDD using higher-order functions
 How to promote regular Python functions to UDFs to run in a distributed fashion
 How to apply UDFs on local data to ease debugging

'''

## Pyspark, freestyle - RDD
'''
I think of an RDD as a bag of elements with no order or relationship to one another. Each element is independent of the other.
The easiest way to experiment with a RDD is to create one from a Python list.

'''

## Promoting a Python list to an RDD

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Python_Spark").getOrCreate()

# Your collection
collection = [1, "two", 3.0, ("four", 4), {"five": 5}]

# Create RDD directly from SparkSession
collection_rdd = spark.sparkContext.parallelize(collection)

# Print RDD
print(collection_rdd)

'''
Our collection_rdd object is effectively an RDD. PySpark returns the type of the
collection when we print the object.
'''

## Manipulating data the RDD way: map(), filter(), and reduce()

'''
An RDD provides many methods (which you can find in the API documentation
for the pyspark.RDD object), but we put our focus on three specific methods: map(),
filter(), and reduce().

map(), filter(), and reduce() all take a function (that we will call f) as their
only parameter and return a copy of the RDD with the desired modifications.


'''
## Mapping a simple function, add_one(), to each element

from py4j.protocol import Py4JJavaError

def add_one(value):
    return value + 1

collection_rdd.map(add_one)

try:
    print(collection_rdd.count())
except Py4JJavaError:
    pass


## Mapping safer_add_one() to each element in an RDD

collection_rdd = spark.sparkContext.parallelize(collection)

def safer_add_one(value):
    try:
        return value + 1
    except TypeError:
        return value
    

collection_rdd1 = collection_rdd.map(collection)

print(collection_rdd.count())

print(collection)


'''
The RDD version of filter() is a little different than the data frame version: it takes a function f, which applies to each object (or ele-
ment) and keeps only those that return a truthful value.

The isinstance() function returns True if the first argument’s type is present in the second argument;
in our case, it’ll test if each element is either a float or an int.

'''


collection_rdd = collection_rdd.filter(lambda elem: isinstance(elem,(float,int)))

print(collection_rdd.collect())

'''
Just like map(), the function passed as a parameter to filter() is applied to every element in the RDD. This time, though, instead of returning the result in a new RDD, we
keep the original value if the result of the function is truthy. If the result is falsy, we drop the element.


'''                                     

# TWO ELEMENTS COME IN, ONE COMES OUT: REDUCE()

'''
PySpark will apply the function to the first two elements, then apply it again to the result and the third element, and so on, until there are no elements left.

'''
## Applying the add() function via reduce()

from operator import add, abs

collection_rdd = spark.sparkContext.parallelize([3,4,5,6,7,8])

print(collection_rdd.reduce(add))

'''

Because of PySpark’s distributed nature, the data of an RDD can be distributed across multiple partitions. The reduce() function will be applied independently on
each partition, and then each intermediate value will be sent to the master node for the final reduction. Because of this, you need to provide a commutative and associa-
tive function to reduce().


A commutative function is a function where the order in which the arguments are applied is not important. For example, add() is commutative, since a + b = b + a.
Oh the flip side, subtract() is not: a - b != b - a.

An associative function is a function where how the values are grouped is not import-ant. add() is associative, since (a + b) + c = a + (b + c). subtract() is not:
(a - b) - c != a - (b - c).add(), multiply(), min(), and max() are both associative and commutative.

'''

a_rdd = spark.sparkContext.parallelize([0, 1, None, [], 0.0])
a_rdd.filter(lambda x: x).collect()

## Using Pyhton to extend Pyspark via UDFs

'''
Unlike the RDD, the data frame has a structure enforced by columns. To address this constraint, PySpark provides the possibility of creating UDFs via the pyspark.sql.func-
tions.udf() function.

'''

## Creating a DataFrame containing a single array column

from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
import pyspark.sql.types as T

fractions = [[x,y] for x in range(100) for y in range(1,100)]

fractions

frac_df = spark.createDataFrame(
    fractions,["numerator", "denominator"]
)

frac_df = frac_df.select(
    F.array(F.col("numerator"),F.col("denominator")).alias("fraction"),
)

frac_df.show(5,False)

'''
Using python with pyspark

- Create and document function
- Make sure the input and output types are compitable
- Test the function

'''
## Creating our three Python functions

'''
We rely on the Fraction data type from the fractions module
to avoid reinventing the wheel.

'''
from fractions import Fraction
from typing import Tuple,Optional

'''
We create a type synonym: Frac. This is equivalent to telling Python/mypy,
“When you see Frac, assume it’s a Tuple[int, int]” (a tuple containing
two integers). This makes the type annotations easier to read.

'''

Frac = Tuple[int, int]

def py_reduce_fraction(frac:Frac) -> Optional[Frac]:
    """

    Args:
        frac (Frac): _description_

    Returns:
        Optional[Frac]: _description_
        
    Reduce a fraction represented as a 2-tuple of integers
    
    """
    num,denom = frac
    
    if denom :
        answer = Fraction(num,denom)
        return answer.numerator,answer.denominator
    return None

assert py_reduce_fraction((3,6)) == (1,2)
assert py_reduce_fraction((1,0)) is None


def py_fraction_to_float(frac:Frac) -> Optional[float]:
    """
    Transform a fraction represented as 2-tuple of integer into a float
    
    """
    num,denom = frac
    
    if denom :
        return float(num)/denom
    return None

assert py_fraction_to_float((2,8)) == 0.25
assert py_fraction_to_float((10,0)) is None

## From Python function to UDFs using udf()

'''
Once you have your Python function created, PySpark provides a simple mechanism
to promote to a UDF. This section covers the udf() function and how to use it directly
to create a UDF, as well as using the decorator to simplify the creation of a UDF.

The function takes two parameters:
- The function you want to promote
- The return type of the generated UDF


I promote the py_reduce_fraction() function to a UDF via the udf()
function. Just like I did with the Python equivalent, I provide a return type to the UDF
(this time, an Array of Long, since Array is the companion type of the tuple and Long
is the one for Python integers). Once the UDF is created, we can apply it just like any
other PySpark function on columns.

'''
## Creating a UDF explicitly with the udf() function

SparkFrac = T.ArrayType(T.LongType())

'''
I promote my Python function using the udf()
function, passing my SparkFrac-type alias as the return type.
'''
reduce_fraction = F.udf(py_reduce_fraction,SparkFrac)

frac_df = frac_df.withColumn(
    "reduced_fraction",reduce_fraction(F.col("fraction"))
)

frac_df.show(5,False)

'''
In this chapter, we effectively tied Python and Spark in the tightest way possible. With
the RDD, you have full control over the data inside the container, but you also have
the responsibility to create functions and use higher-order functions such as map(),
filter(), and reduce() to process the data objects inside.


'''

## Summary

'''
1.The resilient distributed dataset allows for better flexibility compared to the records and columns approach of the data frame.

2.The most low-level and flexible way of running Python code within the distributed Spark environment is to use the RDD. With an RDD, you have no structure
imposed on your data and need to manage type information in your program and defensively code against potential exceptions.

3. The API for data processing on the RDD is heavily inspired by the MapReduce framework. You use higher-order functions such as map(), filter(), and
reduce() on the objects of the RDD.

4. The data frame’s most basic Python code promotion functionality, called the (PySpark) UDF, emulates the “map” part of the RDD. You use it as a scalar func-
tion, taking Column objects as parameters and returning a single Column.

'''






