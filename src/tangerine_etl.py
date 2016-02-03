from pyspark import SparkContext, SparkConf
import re
import json

def parseResults(txt):
	#find all of the GetCartItem Results in the file
	regex = "MainProcess ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).* Content: (\{.*\"carts\".*\})"
	match = re.search(regex, txt)
	#return a list date/time, result tuples
	result = []
	
	if match != None:
		timestamp = match.group(1)
		#parse the GetCartItems result json
		get_cart_item_response = json.loads(match.group(2))
		carts = get_cart_item_response["carts"]
		for cart in carts:
			result.append((timestamp, cart["Name"]))			
	return result
	
if __name__ == "__main__":

	sc=SparkContext(appName="MyApp")

	f = sc.textFile("my-dir/logs")
	rdd = f.flatMap(lambda line: parseResults(line)).map(lambda value: value[0] + ", " + value[1])
	rdd.saveAsTextFile("my-dir/etl");
	sc.stop()


