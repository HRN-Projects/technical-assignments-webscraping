import requests, pandas as pd, time, platform
from scrapy.http import HtmlResponse
from sqlalchemy import create_engine

class AceHardware:
	def __init__(self):
		# Global initialized variables
		self.init_url = "https://www.acehardware.com/departments/outdoor-living/grills-and-smokers/gas-grills?pageSize=100&startIndex={}&_mz_partial=true&includeFacets=false"
		if platform.system().lower() == 'windows':
			self.init_headers = {
				'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36"
			}
		elif platform.system().lower() == 'linux':
			self.init_headers = {
				'user-agent':"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0"
			}

		self.today = time.strftime("%Y-%m-%d")		
		# Setting output file name
		self.outfile_name = "acehardware_{}.csv".format(self.today)		
		# Setting output path on pre-built dir
		self.outfile_path = "../output/{}".format(self.outfile_name)		
		# Setting path on pre-built dir where images will be saved
		self.images_dir_path = "../output/images/{}.jpg"
		self.session = requests.Session()
		self.init_offset = 0
		self.items_per_page = 100
		self.all_items = []


	# Function "get_xpath_obj":
	# 	Convert the webpage response text to xpath object
	# Params:
	# 	1. reqUrl - Requests URL
	# 	2. respText - the response received after above url is hit
	# return: 
	# 	1. xresp (type: xPath object)
	def get_xpath_obj(self, reqUrl, respText):
		xresp = HtmlResponse(url=reqUrl, body=respText, encoding='utf-8')
		return xresp


	# Function "get_engine_obj":
	# 	Builds connection to DB and creates an engine object
	# Params:
	# 	None
	# return: 
	# 	1. engine (DB connection) object
	def get_engine_obj(self):
		engine = create_engine('mysql+mysqldb://{user}:{pw}@{host}:3306/{db}?charset=utf8mb4'.format(user="your_username", pw="your_password", host="your_host_address", db="your_db_name"), echo = False)
		return engine


	# Function "clean_text":
	# 	Clean the input text by removing newlines and extra leading-trailing spaces
	# Params:
	# 	1. text - subject string which needs to be cleaned
	# return: 
	# 	1. text (type: string)
	def clean_text(self, text):
		text = text.replace("\n","").strip()
		return text


	# Function "start_process": 
	# 	Initial function to run and iterate over category pages.
	# Params: None
	# return: None
	def start_process(self):
		offset = self.init_offset
		while(True):										# Iterate on category page, till we get blank response
			url = self.init_url.format(offset)
			print("\nCategory URL: {}".format(url))
			init_resp = self.session.get(url, headers=self.init_headers)
			init_xresp = self.get_xpath_obj(url, init_resp.text)

			prods = init_xresp.xpath("//li[contains(@class,'productlist-item')]")
			if len(prods) > 0:
				for prod in prods:							# Iterate over products
					prod_url = prod.xpath(".//a[@class='mz-productlisting-title']/@href").get()
					self.get_prod_info(prod_url)
			else:
				break
			offset += self.items_per_page

		self.create_output()


	# Function "get_prod_info": 
	# 	Gathering required product details from product page and push them to global list
	# Params: 
	# 	1. prod_url - Product URL from which the product information is to be collected
	# return: None
	def get_prod_info(self, prod_url):
		time.sleep(2)
		print("\nProduct URL: \n{}".format(prod_url))
		resp = self.session.get(prod_url, headers=self.init_headers)
		xresp = self.get_xpath_obj(prod_url, resp.text)

		# Creating a dictionary of all collected attributes for a product.
		# Attributes are collected on basis of xPath selectors. 
		prod_info = {
			'item_id':self.clean_text(xresp.xpath("//div[contains(@class,' product-padding')]//dd[@itemprop='sku']//text()").get()),
			'item_name': self.clean_text(xresp.xpath("//div[contains(@class,' product-padding')]//h1[@class='mz-pagetitle']//text()").get()),
			'item_category': prod_url.split("/")[-3],
			'item_description': "; ".join(xresp.xpath("//div[@id='mobileProductDetailsContainer']//text()").extract()),
			'item_price': self.clean_text(xresp.xpath("//div[@class='pdpSectionPrice']//span[@itemprop='price']//text()").get()),
			'item_image': "https:{}".format(self.clean_text(xresp.xpath("//div[contains(@class,'mz-pdp-noslick-product-image')]//img//@src").get()))
		}

		# Push the product information dictionary to global list
		self.all_items.append(prod_info)
		print("New prod added : {} - {}".format(prod_info['item_id'], prod_info['item_name']))

		# Collect and save product image, named as - 'item_id'.jpg.
		# Saving image to a pre-defined output path
		img = self.session.get(prod_info['item_image'])
		img_path = self.images_dir_path.format(prod_info['item_id'])
		open(img_path, "wb").write(img.content)
		print("Image saved to - {}".format(img_path))


	# Function "create_output": 
	# 	Pushing the collected product information list to output file and DB
	# Params: None
	# return: None
	def create_output(self):
		# Create a Pandas DataFrame from the List of product information dictionary
		column_names = ['item_id', 'item_name', 'item_category', 'item_description', 'item_price', 'item_image']
		out_df = pd.DataFrame(self.all_items, columns=column_names)

		# Create CSV output from DataFrame and save to the pre-defined path
		print("\nCreating output file...")
		out_df.to_csv(self.outfile_path, index=False)
		print("Output file created.")

		# Push the DataFrame to DB
		print("\nPushing Data to DB...")
		data_df.to_sql("your_table_name", con=self.get_engine_obj(), index=False, if_exists='append', chunksize = 1000)
		print("Data uploaded to DB.")	

		print("\n=== Completed ===")


if __name__ == "__main__":
	# Create object of 'AceHardware' class
	obj = AceHardware()
	# Call 'start_process' function to initiate the crawling process
	obj.start_process()