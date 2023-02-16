# Flink Streaming Module


**WIP**

A model for data streaming that can be used in Big-Data scenarios the Flink version.

I have uploaded the Spark version, this is the Flink version, 

check the spark version here : 

Similar to the spark verison, this is also meant to deal with the same situation, and the same mock data.

The mock data of this project was made to suit the template of ordinary e-shops, hence this project has adapt to it accordingly. The model should be able to deal with data streaming for online stores and e-shops as it was designed to deal with data came in the forms of JSON and from relational database.

Hasn't done any real-case test yet. 



## Mock-Data

It doesn't matter which type of data is intended to be streamed, as long as the modules changes accordingly.

This project is designed to deal with two types of data:

1. Business data that generated by actual processes of business, usually stored in databases before visualization.
2. log data, also known as behavior data, most likely in JSON.

The mock data jar is from :

[atguigu.com]: http://www.atguigu.com/	"you might need to know Chinese!"



log data are like this, It is more detailed than the spark version in this module : 

```json
{
    "common":{
        "ar":"110000",
        "ba":"Xiaomi",
        "ch":"xiaomi",
        "is_new":"0",
        "md":"Xiaomi 9",
        "mid":"mid_20",
        "os":"Android 11.0",
        "uid":"47",
        "vc":"v2.1.134"
    },
    "page":{
        "during_time":13968,
        "last_page_id":"home",
        "page_id":"search"
    },
    "ts":1614575952000
}

```

Business data are often prestored in databases such as MySQL, the mock data that this project dealt with is the simplified model of ecommerce companies.

Table examples like : 

order_detail : order_id, sku_id, order_prices, img_url, sku_num, etc.

order_info : consignee, address_delivery, order_comment, tracking_no,etc. 

user_info : login_name, user_level, birthday, gender

etc.

## Data flow



![1632809499071](C:\Users\A\AppData\Roaming\Typora\typora-user-images\1632809499071.png)

## Framework implemented

Flink 

MySQL 5.7

zookeeper

kafka       

Hbase 2.0

Springboot

Maxwell 

## Demands

Different from the spark version, 

The first mission is to create the segmented data warehouse with six layers :



|      | Description                                          | tools      | storage      |
| ---- | :--------------------------------------------------- | ---------- | ------------ |
| ODS  | original data                                        | maxwell    | kafka        |
| DWD  | to different topics in kafka                         | FLINK      | kafka        |
| DWM  | process the data to prepare them for calculation     | FLINK      | kafka        |
| DIM  | Dimensional data                                     | FLINK      | HBase        |
| DWS  | based on some of the dimension to aggregate (mildly) | FLINK      | Clickhouse   |
| ADS  | Prepare the data for visualization                   | Clickhouse | Visual tools |



## Packages
**WIP**
### common  

to put all the constants.



## Notes

1. **Has not been tested in real cases!**

2. This is  mostly used for personal tests.

3. Some annotations haven't been added yet.

   

   

 
