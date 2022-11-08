from __future__ import print_function
if __name__ == '_main__':
    from pyspark.sql import SparkSession
    spark = SparkSession.builder\
                        .master('local')\
                        .appName('Word Count')\
                        .config('spark.some.config.option', 'some-value')\
                        .getOrCreate()

    print(spark.range(5000).where('id > 500').selectExpr('sum(id)').collect())


"""
When you do this, you're going to get a SparkSession that you can pass around your application. It is best 
practice to pass around this variable at runtime rather than instantiating it within every python class

"""