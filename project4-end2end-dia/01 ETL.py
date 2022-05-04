# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)
spark.conf.set('wallet.address',wallet_address)
spark.conf.set('start.date',start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in ethereumetl

# COMMAND ----------

blockDF = spark.sql("select * from ethereumetl.blocks")
transactionDF = spark.sql("select * from ethereumetl.transactions")
receiptDF = spark.sql("select * from ethereumetl.receipts")

# COMMAND ----------

contractDF = spark.sql("select * from ethereumetl.contracts")
silvercontractDF = spark.sql("select * from ethereumetl.silver_contracts")
tokenDF = spark.sql("select * from ethereumetl.tokens")
tokentransferDF = spark.sql("select * from ethereumetl.token_transfers")
tokenpriceDF = spark.sql("select * from ethereumetl.token_prices_usd")

# COMMAND ----------

from pyspark.sql.functions import col,count

# COMMAND ----------

erc20_contract = silvercontractDF.filter(silvercontractDF["is_erc20"]==True)

# COMMAND ----------

erc20_contract.repartition(1).write.format("delta").mode("overwrite").save(BASE_DELTA_PATH)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE erc20_contract
USING DELTA
LOCATION "{BASE_DELTA_PATH}"
"""
)

# COMMAND ----------

erc20_token_transfer = tokentransferDF.join(silvercontractDF, tokentransferDF.token_address==silvercontractDF.address,how="left").filter(silvercontractDF["is_erc20"]==True).drop("address","bytecode","is_erc721")

# COMMAND ----------

token_transfer_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/tftables/"
dbutils.fs.mkdirs(token_transfer_delta_dir)

# COMMAND ----------

erc20_token_transfer.write.format("delta").mode("overwrite").save(token_transfer_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE erc20_token_transfer
USING DELTA
LOCATION "{token_transfer_delta_dir}"
"""
)

# COMMAND ----------

transaction = transactionDF.select("hash",col("from_address").alias("transaction_from_address"),col("to_address").alias("transaction_to_address"),col("value").alias("transaction_value"),"gas","gas_price")

# COMMAND ----------

erc20_tokentransferDF = spark.sql("select * from G06_db.erc20_token_transfer")

# COMMAND ----------

block_select = blockDF.filter(from_unixtime(col("timestamp"),"yyyy-mm-dd HH:mm:ss")>=start_date)

# COMMAND ----------

erc20_token_transfer_block = erc20_tokentransferDF.join(block_select,erc20_tokentransferDF.block_number==block_select.number,"inner")

# COMMAND ----------

erc20_token_transfer_block = erc20_token_transfer_block.select("token_address","from_address","to_address","value","transaction_hash","block_number","timestamp","difficulty","total_difficulty","gas_limit","gas_used","transaction_count")

# COMMAND ----------

display(erc20_token_transfer_block)

# COMMAND ----------

erc20_token_transfer_block_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/ettbtables/"
dbutils.fs.mkdirs(erc20_token_transfer_block_delta_dir)

# COMMAND ----------

erc20_token_transfer_block.write.format("delta").mode("overwrite").save(erc20_token_transfer_block_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE erc20_token_transfer_block
LOCATION "{erc20_token_transfer_block_delta_dir}"
"""
)

# COMMAND ----------

erc_token_transfer_blockDF = spark.sql("select * from G06_db.erc20_token_transfer_block")

# COMMAND ----------

tokenDF_clean = tokenDF.select("address","symbol","name","total_supply")
tokenpriceDF_clean = tokenpriceDF.select("id",col("symbol").alias("token_symbol"),col("name").alias("token_name"),"asset_platform_id","contract_address","sentiment_votes_up_percentage","sentiment_votes_down_percentage","market_cap_rank","coingecko_rank","coingecko_score","developer_score","community_score","liquidity_score","public_interest_score","price_usd")

# COMMAND ----------

erc20_token_transfer_silver = erc_token_transfer_blockDF.join(tokenDF_clean, erc_token_transfer_blockDF.token_address==tokenDF_clean.address,how="left").join(tokenpriceDF_clean,tokenDF_clean.symbol==tokenpriceDF_clean.token_symbol,how="left").drop("token_symbol","token_name")

# COMMAND ----------

#
erc20_tokentransfer_transaction = erc20_tokentransferDF.join(transaction, erc20_tokentransferDF.transaction_hash==transaction.hash,"inner")

# COMMAND ----------

#
display(erc20_tokentransfer_transaction)

# COMMAND ----------

token_transaction_count = erc20_tokentransferDF.select("token_address","transaction_hash").groupBy("token_address").agg(count("transaction_hash").alias("transaction_counts"))

# COMMAND ----------

display(token_transaction_count)

# COMMAND ----------

token_transaction_count_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/ttctables/"
dbutils.fs.mkdirs(token_transaction_count_delta_dir)

# COMMAND ----------

token_transaction_count.write.format("delta").mode("overwrite").save(token_transaction_count_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE erc20_token_transfer_transaction_count
USING DELTA
LOCATION "{token_transaction_count_delta_dir}"
"""
)

# COMMAND ----------

tokenDF_clean = tokenDF.select("address","symbol","name","total_supply")

# COMMAND ----------

token_tokentransfer = spark.sql("select * from G06_db.token_transfer_block left join ethereumetl.tokens on G06_db.token_transfer_block.token_address==ethereumetl.tokens.address")

# COMMAND ----------

token_tokentransfer = token_tokentransfer.drop("address","decimals")

# COMMAND ----------

display(token_tokentransfer)

# COMMAND ----------

tokenpriceDF_clean = tokenpriceDF.select("id",col("symbol").alias("token_symbol"),col("name").alias("token_name"),"asset_platform_id","contract_address","sentiment_votes_up_percentage","sentiment_votes_down_percentage","market_cap_rank","coingecko_rank","coingecko_score","developer_score","community_score","liquidity_score","public_interest_score","price_usd")

# COMMAND ----------

token_transfer_silver = token_tokentransfer.join(tokenpriceDF_clean, token_tokentransfer.token_address==tokenpriceDF_clean.contract_address,how="left").drop("token_symbol","token_name")

# COMMAND ----------

token_transfer_silver_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/tfstables/"
dbutils.fs.mkdirs(token_transfer_silver_delta_dir)

# COMMAND ----------

display(token_transfer_silver)

# COMMAND ----------

token_transfer_silver.write.format("delta").mode("overwrite").save(token_transfer_silver_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE token_transfer_silver
USING DELTA
LOCATION "{token_transfer_silver_delta_dir}"
"""
)

# COMMAND ----------

token_tokentransferDF = spark.sql("select * from G06_db.token_tokentransfer")

# COMMAND ----------

send_value = erc20_tokentransferDF.groupBy("from_address").sum("value")

# COMMAND ----------

send_value = send_value.select("from_address",col("sum(value)").alias("send_value"))

# COMMAND ----------

receive_value = erc20_tokentransferDF.groupBy("to_address").sum("value")

# COMMAND ----------

receive_value = receive_value.select("to_address",col("sum(value)").alias("receive_value"))

# COMMAND ----------

wallet_balance = send_value.join(receive_value, send_value.from_address==receive_value.to_address,"inner").withColumn("balance",col("receive_value")-col("send_value")).select(col("to_address").alias("wallet"),"balance")

# COMMAND ----------

wallet_balance_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/wbtables/"
dbutils.fs.mkdirs(wallet_balance_delta_dir)

# COMMAND ----------

wallet_balance.write.format("delta").mode("overwrite").save(wallet_balance_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE wallet_balance
USING DELTA
LOCATION "{wallet_balance_delta_dir}"
"""
)

# COMMAND ----------

tokenDF_clean = tokenDF.select("address","symbol","name","decimals","total_supply")
tokenpriceDF_clean = tokenpriceDF.select("id",col("symbol").alias("token_symbol"),col("name").alias("token_name"),"asset_platform_id","contract_address","sentiment_votes_up_percentage","sentiment_votes_down_percentage","market_cap_rank","coingecko_rank","coingecko_score","developer_score","community_score","liquidity_score","public_interest_score","price_usd")

# COMMAND ----------

token_transfer_silver = erc20_tokentransferDF.join(tokenDF_clean,erc20_tokentransferDF.token_address==tokenDF_clean.address,how="left").join(tokenpriceDF_clean,tokenDF_clean.symbol==tokenpriceDF_clean.token_symbol, how="left").drop("token_symbol","token_name","address","log_index","is_erc20")

# COMMAND ----------

erc20token_transfer_silver_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/ettstables/"
dbutils.fs.mkdirs(erc20_token_transfer_silver_delta_dir)

# COMMAND ----------

token_transfer_silver.write.format("delta").mode("overwrite").save(erc20_token_transfer_silver_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE erc20_token_transfer_silver
USING DELTA
LOCATION "{erc20_token_transfer_silver_delta_dir}"
"""
)

# COMMAND ----------

tokentransfer_simple = erc20_tokentransfer_transaction.select("token_address","from_address","value")
token_transfer_silver = tokentransfer_simple.join(token_transaction_count,tokentransfer_simple.token_address==token_transaction_count.address,how="left").join(balance,tokentransfer_simple.from_address==balance.wallet,how="left").select("token_address","wallet","balance","transaction_counts").drop_duplicates()

# COMMAND ----------

tokentransfer_silver_delta_dir = f"/mnt/dscc202-datasets/misc/G06/tokenrec/tstables/"
dbutils.fs.mkdirs(tokentransfer_silver_delta_dir)

# COMMAND ----------

dbutils.fs.ls(tokentransfer_silver_delta_dir)

# COMMAND ----------

token_transfer_silver.write.format("delta").mode("overwrite").save(tokentransfer_silver_delta_dir)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE token_transfer_silver
USING DELTA
LOCATION "{tokentransfer_silver_delta_dir}"
"""
)

# COMMAND ----------

block_silver = blockDF.join(transactionDF, blockDF.hash==transaction.block_hash, how="right").select("")

# COMMAND ----------

dbutils.fs.ls(BASE_DELTA_PATH)

# COMMAND ----------

block_silver.write.format("delta").mode("overwrite").save(BASE_DELTA_PATH)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE block_silver
USING DELTA
LOCATION "{BASE_DELTA_PATH}"
"""
)

# COMMAND ----------

dbutils.fs.ls(BASE_DELTA_PATH)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
