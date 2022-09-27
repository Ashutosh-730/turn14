import requests
import json
import pandas
import time
from os import path


def getJson(pageNo):
    # Replace the prod url
    url = "https://api/items?page=" 
    url += pageNo

    payload={}
    headers = {
    'Content-Type': 'application/json',
    # Update the Bearer Token
    'Authorization': 'Bearer <token from auth_token api>'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    json_data = response.json()['data']

    return json_data


def read_json(filename: str) -> dict:  
    try:
        with open(filename, "r") as f:
            data = json.loads(f.read())
    except:
        raise Exception(f"Reading {filename} file encountered an error")
  
    return data
  

def create_dataframe(data: list) -> pandas.DataFrame: 
    # Declare an empty dataframe to append records
    dataframe = pandas.DataFrame()
  
    # Looping through each record
    for d in data:          
        # Normalize the column levels
        record = pandas.json_normalize(d)
          
        # Append it to the dataframe 
        dataframe = dataframe.append(record, ignore_index=True)
  
    return dataframe


def decoration(x):
    print("\n")
    print("************************************************")
    print("   PageNo: ----------->  ", x)
    print("************************************************")
    print("\n")


def main():

    for pageNo in range(372, 1500):

        decoration(pageNo)
        # Get the data from Turn14 API response
        data = getJson(str(pageNo))
    
        # Generate the dataframe for the array items in 
        # details key 
        dataframe = create_dataframe(data)
    
        # Renaming columns of the dataframe 
        print("Normalized Columns:", dataframe.columns.to_list())
    
        dataframe.rename(columns={
            "attributes.product_name":          "product_name",
            "attributes.part_number":           "part_number",
            "attributes.mfr_part_number":       "mfr_part_number",
            "attributes.part_description":      "part_description",
            "attributes.category":              "category",
            "attributes.subcategory":           "subcategory",
            "attributes.dimensions.box_number": "box_number",
            "attributes.dimensions.length":     "length",
            "attributes.dimensions.width":      "width",
            "attributes.dimensions.height":     "height",
            "attributes.dimensions.weight":     "weight",
            "attributes.brand_id":              "brand_id",
            "attributes.brand":                 "brand",
            "attributes.price_group_id":        "price_group_id",
            "attributes.price_group":           "price_group",
            "attributes.active":                "active",
            "attributes.regular_stock":         "regular_stock",
            "attributes.dropship_controller_id": "dropship_controller_id",
            "attributes.air_freight_prohibited": "air_freight_prohibited",
            "attributes.not_carb_approved": "not_carb_approved",
            "attributes.carb_acknowledgement_required": "carb_acknowledgement_required",
            "attributes.ltl_freight_required": "ltl_freight_required",
            "attributes.epa": "epa",
            "attributes.units_per_sku": "units_per_sku",
            "attributes.clearance_item": "clearance_item",
            "attributes.thumbnail": "thumbnail",
            "attributes.barcode": "barcode",
            "attributes.brand_id": "brand_id",
            "attributes.brand_id": "brand_id",

        }, inplace=True)
    
        print("Renamed Columns:", dataframe.columns.to_list())
    
        if dataframe.empty:
            decoration("DataFrame is Empty. Exiting the Script !!")
            exit()

        if path.isfile('details.csv'):
            # Append data to current csv file
            dataframe.to_csv('details.csv', mode='a', index=False, header=False)
            time.sleep(1)
        else:
            # Convert dataframe to new CSV file
            dataframe.to_csv("details.csv", index=False)
            time.sleep(1)
 
 
if __name__ == '__main__':
    main()
