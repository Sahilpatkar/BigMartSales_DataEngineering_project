import config as conf
import Sparkobject as sparkobj
from pyspark.sql.functions import sum, avg, count, col, concat, log1p
from pyspark.sql import functions as F
from pyspark.sql.window import Window



def main():
    Retail_df = sparkobj.spark.read.options (header= conf.header_value, inferschema= conf.inferschema_value).csv(conf.DataSetPath);

    #Retail_df.show()
    
    

#  Pre processsing of data/data cleaning (handling null values, removing duplicate records, handling bad records)

#Q1 Explain how you would handle missing values in the Item_Weight column by using the mean of Item_Weight for each Item_Type.

    
#op1    windowSpec = Window.partitionBy("Item_Type")

#op1    Retail_df = Retail_df.withColumn("Item_Weight",
#op1            F.when(col("Item_Weight").isNull(),
#op1            F.mean(col("Item_Weight")).over(windowSpec))
#op1            .otherwise(col("Item_Weight")))
#   Retail_df.select('Item_Identifier', 'Item_Weight', 'Item_Type').show();

    

    mean_item_wt = Retail_df.groupBy("Item_Type") \
            .agg(F.mean("Item_Weight").alias("mean_item_wt"))

    Retail_df1 = Retail_df.join(mean_item_wt, "Item_Type", "left")
    
    Retail_df1 = Retail_df1.withColumn("Item_Weight",
    F.when(F.col("Item_Weight").isNull(),
    F.col("mean_item_wt"))
    .otherwise(F.col("Item_Weight")))
     

    Retail_df1.select('Item_Identifier', 'Item_Weight', 'Item_Type').show()
                    

        


#Q2 How do you handle missing values in the Outlet_Size column using the mode of Outlet_Size for each Outlet_Type?

    mode_outlet_size = Retail_df1.groupBy("Outlet_Size") \
                .count() \
                .orderBy(F.desc("count")) \
                .first()[0]

    Retail_df2 = Retail_df1.withColumn("Outlet_Size",
             F.when(F.col("Outlet_Size").isNull(),
                F.lit(mode_outlet_size))
            .otherwise(F.col("Outlet_Size")))
    
    Retail_df2.select('Item_Identifier','Outlet_Size', 'Outlet_Type').show();







#Q3 Describe the process to standardize the Item_Fat_Content column, converting all variations of 'Low Fat' to a single standard and 'Regular' to another standard.

    Retail_df3 = Retail_df2.withColumn("Item_Fat_Content",
        F.when(F.col("Item_Fat_Content").isin (['low fat', 'Low Fat', 'LF']), "Low Fat")
        .when(F.col("Item_Fat_Content").isin (['reg','Regular']), "Regular")
        .otherwise(F.col("Item_Fat_Content")))


    Retail_df3.select('Item_Identifier','Item_Fat_Content').show();
    Retail_df3.write.parquet(f"./{conf.outputPath}/Standardize_ItemFatContent", mode="overwrite")
    



#Q4 How do you calculate the age of an outlet using the Outlet_Establishment_Year column and the current year (2024)?

    current_year = 2024
    
    Retail_df3 = Retail_df3.withColumn('Outlet_Age',
            F.lit(current_year) - F.col('Outlet_Establishment_Year'))

    Retail_df3.select('Outlet_Age','Outlet_Establishment_Year').show();



#Q5 Explain the use of StringIndexer for encoding categorical columns like Item_Fat_Content, Outlet_Size, and Outlet_Location_Type.




#Q6 How do you apply OneHotEncoder to encode the columns Item_Type, Outlet_Identifier, Outlet_Size_Index, and Outlet_Location_Type_Index?






#Q7 Write the code to create an interaction feature between Item_Type and Outlet_Identifier.

    Retail_df4 = Retail_df3.withColumn("Item_Outlet_Interaction",
    concat(col("Item_Type"), F.lit("_"), F.col("Outlet_Identifier")))

    Retail_df4.select('Item_Outlet_Interaction').show();
    Retail_df4.write.parquet(f"./{conf.outputPath}/InteractionFeature_ItemOutlet", mode="overwrite")




#Q8 How do you assemble the feature columns Item_Weight, Item_Visibility, Item_MRP, Outlet_Age, and the encoded columns into a single feature vector using VectorAssembler



#Q9 Explain the purpose of scaling features and demonstrate how to use StandardScaler to scale the assembled feature vector




#Q10    Describe the process of handling outliers in the Item_Visibility column by using the interquartile range (IQR) method. Provide the code for it.

    quantiles = Retail_df4.approxQuantile("Item_Visibility", [0.25, 0.75], 0.0)
    Q1, Q3 = quantiles[0], quantiles[1]

    IQR = Q3-Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    Retail_df5 = Retail_df3.filter((col("Item_Visibility") >= lower_bound) & (col("Item_Visibility") <= upper_bound))

    Retail_df5.show();
    Retail_df5.write.parquet(f"./{conf.outputPath}/HandleOutliers_ItemVisibility", mode="overwrite")


#Q11    How would you apply a log transformation to the Item_MRP column and write the corresponding PySpark code?

    Retail_df6 = Retail_df4.withColumn("Item_MRP_log", log1p(col("Item_MRP")))
    
    Retail_df6.select('Item_MRP', 'Item_MRP_log').show();
    Retail_df6.write.parquet(f"./{conf.outputPath}/LogTransformation_ItemMRP", mode="overwrite")



#Q12    How do you bin the Item_Visibility column into categories like 'Low', 'Medium', and 'High'?
    
    percentiles = Retail_df.approxQuantile("Item_Visibility", [0.33, 0.66], 0.0)
    low_threshold = percentiles[0]
    high_threshold = percentiles[1]

    Retail_df5 = Retail_df5.withColumn("Visibility_Category", 
        F.when(col("Item_Visibility") <= low_threshold, "Low")
        .when((col("Item_Visibility") > low_threshold) & (col("Item_Visibility") <= high_threshold), "Medium")
        .otherwise("High")
    )

    Retail_df5.select('Item_Visibility','Visibility_Category').show();
    Retail_df5.write.parquet(f"./{conf.outputPath}/RangeCategorization_on_ItemVisibility", mode="overwrite")
    


#Q13    Demonstrate the use of StringIndexer to encode the binned Item_Visibility feature.




#Q14    Write the PySpark code to compute the total sales, average sales, and total items sold grouped by Item_Type.

    Retail_df6 = Retail_df5.groupBy("Item_Type").agg(sum('Item_MRP').alias('Total_Sales'),
                avg('Item_MRP').alias('Average_Sales'),
                count('Item_Identifier').alias('Total_items_sold'))

    Retail_df6.show();
    Retail_df6.write.parquet(f"./{conf.outputPath}/SalesDetails_based_Item_Type", mode="overwrite")
    

#Q15    Write the PySpark code to compute the total sales, average sales, and total items sold grouped by Outlet_Identifier.

    Retail_df7 = Retail_df5.groupBy("Outlet_Identifier").agg(sum('Item_MRP').alias('Total_Sales'),
                avg('Item_MRP').alias('Average_Sales'),
                count('Item_Identifier').alias('Total_items_sold'))

    Retail_df7.show()
    Retail_df7.write.parquet(f"./{conf.outputPath}/SalesDetails_based_Outlet_Identifier", mode="overwrite")



#Q16    How do you calculate the average sales per year based on Outlet_Establishment_Year?


    Retail_df8 = Retail_df5.groupBy("Outlet_Establishment_Year").agg(avg('Item_MRP').alias('Average_Sales')).orderBy("Outlet_Establishment_Year")

    Retail_df8.write.parquet(f"./{conf.outputPath}/AverageSales_basedOn_Outlet_Establishment_Year", mode="overwrite")


#Q17    Write the PySpark code to compute the total sales, average sales, and total items sold grouped by Item_Fat_Content

    
    Retail_df9 = Retail_df5.groupBy("Item_Fat_Content").agg(sum('Item_MRP').alias('Total_Sales'),
                avg('Item_MRP').alias('Average_Sales'),
                count('Item_Identifier').alias('Total_items_sold'))

    Retail_df9.show();
    Retail_df9.write.parquet(f"./{conf.outputPath}/Sales_based_Item_Fat_Content", mode="overwrite")


main()