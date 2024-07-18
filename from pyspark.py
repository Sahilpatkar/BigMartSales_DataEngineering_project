from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, concat, log1p
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config as conf
import Sparkobject as sparkobj

def main():
    Retail_df = sparkobj.spark.read.options(header=conf.header_value, inferschema=conf.inferschema_value).csv(conf.DataSetPathDataSetPath)
    
    # Preprocessing of data/data cleaning (handling null values, removing duplicate records, handling bad records)
    
    # Q1: Handle missing values in the Item_Weight column by using the mean of Item_Weight for each Item_Type.
    mean_item_wt = Retail_df.groupBy("Item_Type").agg(F.mean("Item_Weight").alias("mean_item_wt"))
    Retail_df1 = Retail_df.join(mean_item_wt, "Item_Type", "left")
    Retail_df1 = Retail_df1.withColumn("Item_Weight", F.when(F.col("Item_Weight").isNull(), F.col("mean_item_wt")).otherwise(F.col("Item_Weight")))
    Retail_df1.select('Item_Identifier', 'Item_Weight', 'Item_Type').show()
    Retail_df1.write.parquet("./output2/HandleMissing_ItemWeight", mode="overwrite")
    
    # Q2: Handle missing values in the Outlet_Size column using the mode of Outlet_Size for each Outlet_Type.
    mode_outlet_size = Retail_df1.groupBy("Outlet_Size").count().orderBy(F.desc("count")).first()[0]
    Retail_df2 = Retail_df1.withColumn("Outlet_Size", F.when(F.col("Outlet_Size").isNull(), F.lit(mode_outlet_size)).otherwise(F.col("Outlet_Size")))
    Retail_df2.select('Item_Identifier', 'Outlet_Size', 'Outlet_Type').show()
    Retail_df2.write.parquet("./output2/HandleMissing_OutletSize", mode="overwrite")
    
    # Q3: Standardize the Item_Fat_Content column.
    Retail_df3 = Retail_df2.withColumn("Item_Fat_Content", F.when(F.col("Item_Fat_Content").isin(['low fat', 'Low Fat', 'LF']), "Low Fat").when(F.col("Item_Fat_Content").isin(['reg', 'Regular']), "Regular").otherwise(F.col("Item_Fat_Content")))
    Retail_df3.select('Item_Identifier', 'Item_Fat_Content').show()
    Retail_df3.write.parquet("./output2/Standardize_ItemFatContent", mode="overwrite")
    
    # Q4: Calculate the age of an outlet using the Outlet_Establishment_Year column and the current year (2024).
    current_year = 2024
    Retail_df3 = Retail_df3.withColumn('Outlet_Age', F.lit(current_year) - F.col('Outlet_Establishment_Year'))
    Retail_df3.select('Outlet_Age', 'Outlet_Establishment_Year').show()
    Retail_df3.write.parquet("./output2/Calculate_OutletAge", mode="overwrite")
    
    # Q7: Create an interaction feature between Item_Type and Outlet_Identifier.
    Retail_df4 = Retail_df3.withColumn("Item_Outlet_Interaction", concat(col("Item_Type"), F.lit("_"), F.col("Outlet_Identifier")))
    Retail_df4.select('Item_Outlet_Interaction').show()
    Retail_df4.write.parquet("./output2/InteractionFeature_ItemOutlet", mode="overwrite")
    
    # Q10: Handle outliers in the Item_Visibility column using the interquartile range (IQR) method.
    quantiles = Retail_df4.approxQuantile("Item_Visibility", [0.25, 0.75], 0.0)
    Q1, Q3 = quantiles[0], quantiles[1]
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    Retail_df5 = Retail_df4.filter((col("Item_Visibility") >= lower_bound) & (col("Item_Visibility") <= upper_bound))
    Retail_df5.show()
    Retail_df5.write.parquet("./output2/HandleOutliers_ItemVisibility", mode="overwrite")
    
    # Q11: Apply a log transformation to the Item_MRP column.
    Retail_df6 = Retail_df5.withColumn("Item_MRP_log", log1p(col("Item_MRP")))
    Retail_df6.select('Item_MRP', 'Item_MRP_log').show()
    Retail_df6.write.parquet("./output2/LogTransformation_ItemMRP", mode="overwrite")
    
    # Q12: Bin the Item_Visibility column into categories like 'Low', 'Medium', and 'High'.
    percentiles = Retail_df.approxQuantile("Item_Visibility", [0.33, 0.66], 0.0)
    low_threshold = percentiles[0]
    high_threshold = percentiles[1]
    Retail_df5 = Retail_df5.withColumn("Visibility_Category", F.when(col("Item_Visibility") <= low_threshold, "Low").when((col("Item_Visibility") > low_threshold) & (col("Item_Visibility") <= high_threshold), "Medium").otherwise("High"))
    Retail_df5.select('Item_Visibility', 'Visibility_Category').show()
    Retail_df5.write.parquet("./output2/Bin_ItemVisibility", mode="overwrite")
    
    # Q14: Compute the total sales, average sales, and total items sold grouped by Item_Type.
    Retail_df6 = Retail_df5.groupBy("Item_Type").agg(sum('Item_MRP').alias('Total_Sales'), avg('Item_MRP').alias('Average_Sales'), count('Item_Identifier').alias('Total_items_sold'))
    Retail_df6.show()
    Retail_df6.write.parquet("./output2/SalesDetails_by_ItemType", mode="overwrite")
    
    # Q15: Compute the total sales, average sales, and total items sold grouped by Outlet_Identifier.
    Retail_df7 = Retail_df5.groupBy("Outlet_Identifier").agg(sum('Item_MRP').alias('Total_Sales'), avg('Item_MRP').alias('Average_Sales'), count('Item_Identifier').alias('Total_items_sold'))
    Retail_df7.show()
    Retail_df7.write.parquet("./output2/SalesDetails_by_OutletIdentifier", mode="overwrite")
    
    # Q16: Calculate the average sales per year based on Outlet_Establishment_Year.
    Retail_df8 = Retail_df5.groupBy("Outlet_Establishment_Year").agg(avg('Item_MRP').alias('Average_Sales')).orderBy("Outlet_Establishment_Year")
    Retail_df8.write.parquet("./output2/AverageSales_by_OutletEstablishmentYear", mode="overwrite")
    
    # Q17: Compute the total sales, average sales, and total items sold grouped by Item_Fat_Content.
    Retail_df9 = Retail_df5.groupBy("Item_Fat_Content").agg(sum('Item_MRP').alias('Total_Sales'), avg('Item_MRP').alias('Average_Sales'), count('Item_Identifier').alias('Total_items_sold'))
    Retail_df9.show()
    Retail_df9.write.parquet("./output2/SalesDetails_by_ItemFatContent", mode="overwrite")

main()
