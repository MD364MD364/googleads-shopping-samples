from __future__ import print_function
from asyncio.windows_events import NULL
import sys

import mariadb
import json
import bigcommerce
from pyparsing import nums
import requests
from copy import copy
# The common module provides setup functionality used by the samples,
# such as authentication and unique id generation.
from shopping.content import common
from shopping.content.products.insert_batch import BATCH_SIZE
from datetime import datetime

import re
from slugify import slugify

def isValidNumber(numString):
    if(len(numString) > 0):
        for c in numString:
            if((not c.isdigit()) and (c != '.')):
                return False
    else:
        return False
    return True

def main(argv):
    
    requests = []

    # # Test to see how long BigC calls take to respond
    # api = bigcommerce.api.BigcommerceApi(client_id='gnw0dh7xzx97ck9pw4kwpthvefui412', store_hash='z84xkjcnbz', access_token='tqhvp7fmyqr438pewjwtcwi1vggxpky')
    # current_time = datetime.now().strftime("%H:%M:%S")
    # print("Current Time =", current_time)
    # singleProdBigCInfo = api.Products.get(16097)
    # print(singleProdBigCInfo)
    # current_time = datetime.now().strftime("%H:%M:%S")
    # print("Current Time =", current_time)
    # quit()
    # # End of test

    def submitRequest(requestBody, isLast):
        # print('Submission #'+ str(len(requests)) + ' received')
        print('Submission: ')
        print(requestBody)
        BATCH_SIZE = 50

        if(requestBody != ''):
            requests.append({
                'batchId': len(requests),
                'merchantId': '507522930',
                'method': 'insert',
                'product': requestBody,
                'updateMask': 'gtin,availability,brand,channel,condition,contentLanguage,customAttributes,description,id,imageLink,link,maxHandlingTime,minHandlingTime,mpn,offerId,price,productHeight,productLength,productWeight,productWidth,sellOnGoogleQuantity,targetCountry,title,customLabel0'
                # 'updateMask': 'gtin,availability,brand,channel,condition,contentLanguage,customAttributes,description,id,imageLink,link,maxHandlingTime,minHandlingTime,mpn,offerId,price,productHeight,productLength,productWeight,productWidth,sellOnGoogleQuantity,title,promotionIds'
                })

        if((len(requests) >= BATCH_SIZE) or ((isLast == True) and (len(requests) > 0))):
            try:
                conn4 = mariadb.connect(
                    user="root",
                    password="5LUxZA2CnEmZQ8dm",
                    host="127.0.0.1",
                    port=3306,
                    database="safetymediaapp"
                )
                print('connected')
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)
            cur4 = conn4.cursor()
            # print('Starting submission')
            # startCount = len(requestBody)
            service, config, _ = common.init(argv, __doc__)
            # merchant_id = config['merchantId']

            batch = {
                'entries': requests
            }
            # count = 0
            # for product in requests:

            # batch = {
            #     'entries': [{
            #         'batchId': startCount + i,
            #         'merchantId': merchant_id,
            #         'method': 'insert',
            #         'product': requests[i],
            #         } for i in range(BATCH_SIZE)],
            # }
            # print(json.dumps(batch, indent=4, sort_keys=True))
            finishedProducts = []
            request = service.products().custombatch(body=batch)
            result = request.execute()
            # print(json.dumps(result, indent=4, sort_keys=True))
            if result['kind'] == 'content#productsCustomBatchResponse':
                print('result:')
                print(result)
                entries = result['entries']
                for entry in entries:
                    product = entry.get('product')
                    errors = entry.get('errors')
                    print(errors)
                    if product:
                        # print('Product "%s" with offerId "%s" was created.' %
                        #     (product['id'], product['offerId']))
                        finishedProducts.append(product['offerId'])
                        print('sent ' + product['offerId'])
                        cur4.execute("UPDATE product SET statusMerchantCenter='1' WHERE sku='" + product['offerId'].lstrip().rstrip() + "'")
                        conn4.commit()
                        # cur4.execute("UPDATE product SET statusMerchantCenter='1' WHERE sku='" + temp + "'")
                        # cur4.execute("SELECT statusMerchantCenter FROM product WHERE sku='LI72V'")

                    elif errors:
                        print('Errors for batch entry %d:' % entry['batchId'])
                        print(json.dumps(errors, sort_keys=True, indent=2, separators=(',', ': ')))
            else:
                print('There was an error. Response: %s' % result)
            requests.clear()
            conn4.close()
            # inValues = "('" + ("','".join(finishedProducts)) + "')"

            # sqlCommand = "UPDATE product SET statusMerchantCenter='1' WHERE sku IN " + inValues
            # print(sqlCommand)

    def createItem(line):
        # print('creating item')
        itemDict = {
            'bigc_product_id': line[1],
            'spire_product_id': line[2],
            'product_name': line[3],
            'variant_id': line[4],
            'sku': line[5],
            'description': line[6],
            'price': line[7],
            'categories': line[8],
            'parent_product_code': line[9],
            'is_featured': line[10],
            'availability': line[11],
            'standard_cost': line[12],
            'image_path': line[13],
            'image_url': line[14],
            'image_id': line[15],
            'weight': line[16],
            'buy_measure_code': line[17],
            'availability_description': line[18],
            'images': line[19],
            'visible_udf_data': line[20],
            'material_details': line[21],
            'custom_fields': line[22],
            'updated_custom_fields': line[23],
            'price_matrix': line[24],
            'bigc_variant_productid': line[25],
            'upcs_values': line[26],
            'spire_product_status': line[27],
            'status': line[28],
            'statusMerchantCenter': line[29],
            'created_at': line[30],
            'updated_at': line[31],
            'lastImageUpdate': line[32],
        }
        return itemDict


    def fixURL(rawURL):
        newURL = rawURL.replace(" ", "-")
        newURL = newURL.replace(",", "")
        newURL = newURL.replace(".", "")
        newURL = newURL.replace('"', "")
        newURL = newURL.replace('/', "")

        # newURL = re.sub('/[^A-Za-z0-9\-]/', '', newURL)
        # newURL = re.sub('/-+/', '-', newURL)
        # return newURL
        # print(slugify(newURL))
        return slugify(newURL)
    
    # print(fixURL('Clear Acrylic Frame to Protect 4\"x7\" Page 5.5\"W x 8.5\"H x 0.125\"D'))
    # exit()

    api = bigcommerce.api.BigcommerceApi(client_id='gnw0dh7xzx97ck9pw4kwpthvefui412', store_hash='z84xkjcnbz', access_token='tqhvp7fmyqr438pewjwtcwi1vggxpky')

    v3client = bigcommerce.connection.OAuthConnection(client_id='gnw0dh7xzx97ck9pw4kwpthvefui412',
                                                      store_hash='z84xkjcnbz',
                                                      access_token='tqhvp7fmyqr438pewjwtcwi1vggxpky',
                                                      api_path='/stores/{}/v3/{}')

    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="root",
            password="5LUxZA2CnEmZQ8dm",
            host="127.0.0.1",
            port=3306,
            database="safetymediaapp"
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    totalSubmission = 0
    # Get Cursor
    cur = conn.cursor()
    # cur.execute("SELECT * FROM product WHERE statusMerchantCenter='2' AND visible_udf_data LIKE \'%\"brandname\"\:\"Safety Media\"%\' AND custom_fields LIKE '%\"googlemcavail\"\:true%'")
    cur.execute("SELECT * FROM product WHERE statusMerchantCenter='2' AND custom_fields LIKE '%\"googlemcavail\"\:true%'")
    # cur.execute("SELECT * FROM product WHERE custom_fields LIKE '%\"googlemcavail\"\:true%'")
    productsAll = []
    count = 0
    for (prod) in cur:
        count += 1
        # print(createItem(prod))
        productsAll.append(createItem(prod))
    print(count)
    conn.close()



    counter = 0
    last = False
    for prod in productsAll:
        counter += 1
        if(counter == count):
            last = True

        print('Handling ' + prod['sku'])
        custom_fields = json.loads(prod['custom_fields'])

        # If parent, needs to be ignored. Only single products and variants need to go up
        if(custom_fields['primaryselector'] != prod['sku']):
            # it's not a parent
            # Let's create a base product, then modify for each scenario
            productGeneral = {
                'offerId': prod['sku'],
                'title': prod['product_name'],
                'description': custom_fields['prodhtml'],
                'contentLanguage': 'en', # apparently refers to page language and we only have english, so en it is for now
                'channel': 'online',
                'availability': 'in stock',
                'condition': 'new',
                'sellOnGoogleQuantity': 11,
                'minHandlingTime': int(json.loads(prod['custom_fields'])['RegProdDel']),
                'maxHandlingTime': (json.loads(prod['custom_fields'])['prodelrange'] + int(json.loads(prod['custom_fields'])['RegProdDel'])),
                'productWeight': {'value': prod['weight'], 'unit': 'lb'}
            }

            if('mpn' in json.loads(prod['custom_fields'])):
                if (json.loads(prod['custom_fields'])['mpn'] != ''):
                    productGeneral['mpn'] = json.loads(prod['custom_fields'])['mpn']
                elif (('brandname' in json.loads(prod['visible_udf_data'])) and (json.loads(prod['visible_udf_data'])['brandname'].lower() == "safety media")):
                    productGeneral['mpn'] = prod['sku']
            elif (('brandname' in json.loads(prod['visible_udf_data'])) and (json.loads(prod['visible_udf_data'])['brandname'].lower() == "safety media")):
                productGeneral['mpn'] = prod['sku']

            # If brandname exists in spire, set it
            if(('brandname' in json.loads(prod['visible_udf_data'])) and (json.loads(prod['visible_udf_data'])['brandname'] != "")):
                productGeneral['brand'] = json.loads(prod['visible_udf_data'])['brandname']
            # If promotionIds exists, set it
            print('check promotionIds')
            if(('googlepromoid' in json.loads(prod['custom_fields'])) and (json.loads(prod['custom_fields'])['googlepromoid'] != "")):
                # separates values using a comma
                productGeneral['customLabel0'] = json.loads(prod['custom_fields'])['googlepromoid']
            else:
                productGeneral['customLabel0'] = 'no_label'

            
            # Handle size
            sizeSet = False
            issueWithValues = False
            # If custom size is set, parse that
            # TODOBARDIA Need to consider custom size being in cm or mm as well. Have to check if that even exists
            if((json.loads(prod['custom_fields'])['size'] != "") and (json.loads(prod['custom_fields'])['size'][0].isdigit()) and (('"W' in json.loads(prod['custom_fields'])['size']) or ('mmW' in json.loads(prod['custom_fields'])['size'])) and (('"H' in json.loads(prod['custom_fields'])['size']) or ('mmH' in json.loads(prod['custom_fields'])['size']))):
                # there is custom size
                a = 0
                dimensions = json.loads(prod['custom_fields'])['size'].split('x')
                for dimension in dimensions:
                    dimUnit = ''
                    if('"' in dimension):
                        dim = dimension.split('"')[0].lstrip().rstrip()
                        dimUnit = 'in'
                    elif('mm' in dimension):
                        dim = dimension.split('mm')[0].lstrip().rstrip()
                        dimUnit = 'mm'
                    if(not isValidNumber(dim)):
                        issueWithValues = True
                    match a:
                        case 0:
                            # 'set width'
                            if dimUnit == 'in':
                                productGeneral['productWidth'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productWidth'] = {'value': int(dim)/10.0, 'unit': 'cm'}
                            sizeSet = True
                        case 1:
                            #'set height'
                            if dimUnit == 'in':
                                productGeneral['productHeight'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productHeight'] = {'value': int(dim)/10.0, 'unit': 'cm'}
                        case 2:
                            # 'set depth'
                            if dimUnit == 'in':
                                productGeneral['productLength'] = {'value': dim, 'unit': 'in'}
                            elif dimUnit == 'mm':
                                productGeneral['productLength'] = {'value': int(dim)/10.0, 'unit': 'cm'}
                    a+=1
                if(a == 2):
                    # means there was no depth. Need to grab depth from dimensions tab of the UDFs
                    if(json.loads(prod['custom_fields'])['depth'] != NULL):
                        productGeneral['productLength'] = {'value': json.loads(prod['custom_fields'])['depth'], 'unit': 'in'}
            
            if(issueWithValues):
                sizeSet = False
            
            if((sizeSet == False) and (json.loads(prod['custom_fields'])['primaryselector'] != "")):
                # custom size field was not filled, so we'll be using the selector reference instead
                # there is a primaryselector
                if('productWidth' in productGeneral):
                    del productGeneral['productWidth']
                if('productHeight' in productGeneral):
                    del productGeneral['productHeight']
                if('productLength' in productGeneral):
                    del productGeneral['productLength']
                try:
                    conn2 = mariadb.connect(
                        user="root",
                        password="5LUxZA2CnEmZQ8dm",
                        host="127.0.0.1",
                        port=3306,
                        database="safetymediaapp"
                    )
                except mariadb.Error as e:
                    print(f"Error connecting to MariaDB Platform: {e}")
                    sys.exit(1)
                # using second cursor
                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT custom_fields FROM product WHERE sku='" + prod['parent_product_code'] + "'")
                for (parent) in cur2:
                    if('size' in json.loads(parent[0])['selectorref'].lower()):
                        position = json.loads(parent[0])['selectorref'].lower()[0 : json.loads(parent[0])['selectorref'].lower().index('size')].count(',') + 1
                        dropdownDimensions = json.loads(prod['custom_fields'])['ref' + str(position)]
                        if(('w' in dropdownDimensions.lower()) and ('h' in dropdownDimensions.lower())):
                            # at least width and length are in
                            dimCount = 0
                            dimensions = dropdownDimensions.split('x')
                            for dimension in dimensions:
                                if('"' in dimension):
                                    amount = float(dimension.split('"')[0].lstrip().rstrip())
                                    unit = 'in'
                                elif('mm' in dimension.lower()):
                                    amount = float(dimension.split('mm')[0].lstrip().rstrip()) / 10
                                    unit = 'cm'
                                elif('cm' in dimension.lower()):
                                    amount = float(dimension.split('cm')[0].lstrip().rstrip())
                                    unit = 'cm'
                                
                                match dimCount:
                                    case 0:
                                        # send width
                                        productGeneral['productWidth'] = {'value': amount, 'unit': unit}
                                        sizeSet = True
                                    case 1:
                                        # send height
                                        productGeneral['productHeight'] = {'value': amount, 'unit': unit}
                                    case 2:
                                        # send depth
                                        productGeneral['productLength'] = {'value': amount, 'unit': unit}
                                        
                                # print(str(amount) + unit)
                                dimCount += 1
                conn2.close()
            if((sizeSet == False)):
                # Grab straight from dimensions tab of spire
                # Resorted to shipping sizes
                productGeneral['productWidth'] = {'value': json.loads(prod['custom_fields'])['Width'], 'unit': 'in'}
                productGeneral['productHeight'] = {'value': json.loads(prod['custom_fields'])['Height'], 'unit': 'in'}
                productGeneral['productLength'] = {'value': json.loads(prod['custom_fields'])['depth'], 'unit': 'in'}
                sizeSet = True
            
            if(sizeSet == False):
                print(prod['sku'] + 'None of the size methods worked. Something is wrong')
                # quit()
            
            # Handle customAttributes
            totalCustoms = []
            if(json.loads(prod['custom_fields'])['includes'] != ""):
                totalCustoms.append(json.loads(prod['custom_fields'])['includes'])
            if(json.loads(prod['custom_fields'])['selfadhesivesticker'] == True):
                totalCustoms.append('Self-Adhesive Sticker')
            if(json.loads(prod['custom_fields'])['2sidedtape'] == True):
                totalCustoms.append('2 Sided Tape')
            if(json.loads(prod['custom_fields'])['slotholes'] == True):
                totalCustoms.append('Slot Holes')
            if(json.loads(prod['custom_fields'])['holes'] == True):
                totalCustoms.append('Holes')
            if(json.loads(prod['custom_fields'])['screws'] == True):
                totalCustoms.append('Screws')
            if(len(totalCustoms) != 0):
                productGeneral['customAttributes'] = {"name": "Includes", "value": (", ".join(totalCustoms))}
                # productGeneral['customAttributes'] = ",".join(totalCustoms)

            
            # Handle material
            if(json.loads(prod['custom_fields'])['material'] != ""):
                productGeneral['material'] = json.loads(prod['custom_fields'])['material']
            else:
                # Need to check if it's a variant with a selector dropdown of material
                if(json.loads(prod['custom_fields'])['primaryselector'] != ""):
                    # there is a primaryselector
                    try:
                        conn2 = mariadb.connect(
                            user="root",
                            password="5LUxZA2CnEmZQ8dm",
                            host="127.0.0.1",
                            port=3306,
                            database="safetymediaapp"
                        )
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)
                    cur2 = conn2.cursor()
                    cur2.execute(
                        "SELECT custom_fields FROM product WHERE sku='" + prod['parent_product_code'] + "'")
                    for (parent) in cur2:
                        if('material' in json.loads(parent[0])['selectorref'].lower()):
                            position = json.loads(parent[0])['selectorref'].lower()[0 : json.loads(parent[0])['selectorref'].lower().index('material')].count(',') + 1
                            productGeneral['material'] = json.loads(prod['custom_fields'])['ref' + str(position)]



            # check if variant
            if(prod['variant_id'] is not None):
                try:
                    # Variant
                    # need to figure out the link from querying the parent\
                    try:
                        conn3 = mariadb.connect(
                            user="root",
                            password="5LUxZA2CnEmZQ8dm",
                            host="127.0.0.1",
                            port=3306,
                            database="safetymediaapp"
                        )
                    except mariadb.Error as e:
                        print(f"Error connecting to MariaDB Platform: {e}")
                        sys.exit(1)


                    # Second cursor to pull the needed info from the parent
                    cur2 = conn3.cursor()
                    cur2.execute(
                        "SELECT custom_fields, bigc_product_id FROM product WHERE sku='" + prod['parent_product_code'] + "'")
                    for (parent) in cur2:
                        # variantURLCA = 'https://safetymedia.com/' + json.loads(parent[0])['seourl'].replace(' ', '-').replace("'","").replace('"','').replace(',','').lower() + '?setCurrencyId=1&sku=' + prod['sku']
                        variantURLCA = 'https://safetymedia.com/' + fixURL(json.loads(parent[0])['seourl']).lower() + '?setCurrencyId=1&sku=' + prod['sku']
                        # variantURLUS = 'https://safetymedia.com/' + json.loads(parent[0])['seourl'].replace(' ', '-').replace("'","").replace('"','').replace(',','').lower() + '?setCurrencyId=2&sku=' + prod['sku']
                        variantURLUS = 'https://safetymedia.com/' + fixURL(json.loads(parent[0])['seourl']).lower() + '?setCurrencyId=2&sku=' + prod['sku']
                        parentBigcID = parent[1]
                    conn3.close()

                    
                    variantBigCInfo = v3client.get('/catalog/products/' + str(parentBigcID) + '/variants/' + str(prod['variant_id']))

                    # CA
                    variantCA = copy(productGeneral)

                    variantCA['id'] = 'online:EN:CA:' + prod['sku']
                    variantCA['link'] = variantURLCA
                    variantCA['imageLink'] =  variantBigCInfo['data']['image_url']
                    variantCA['targetCountry'] = 'CA'
                    variantCA['price'] = {'value': prod['price'], 'currency': 'CAD'}
                    variantCA['itemGroupId'] = prod['parent_product_code']
                    variantCA['customLabel0'] = productGeneral['customLabel0'] + '-CA'
                    variantCA['customLabel0'] = 'test'
                    # If gtin exists, set it
                    # TODOBARDIA IF BLANK,CHECK ISBN NUMBER, IF ISBN BLANK, PUT BLANK. IF EITHER EXISTS, SET IDENTIFIER TRUE,
                    if(('upc' in prod['upcs_values']) and (json.loads(prod['upcs_values'])['upc'] != '')):
                        variantCA['gtin'] = json.loads(prod['upcs_values'])['upc']
                        variantCA['identifierExists'] = 'True'
                    elif(('isbn' in prod['custom_fields']) and (json.loads(prod['custom_fields'])['isbn'] != '')):
                        variantCA['gtin'] = json.loads(prod['custom_fields'])['isbn']
                        variantCA['identifierExists'] = 'True'

                    # print(variantCA)
                    # Construct the service object to interact with the Content API.
                    # service, config, _ = common.init(argv, __doc__)

                    # Get the merchant ID from merchant-info.json.
                    # merchant_id = config['merchantId']

                    # Create the request with the merchant ID and product object.
                    # request = service.products().insert(merchantId=merchant_id, body=variantCA)

                    # # Execute the request and print the result.
                    # result = request.execute()
                    # print('Product with offerId "%s" was updated.' %
                    #       (result['offerId']))
                    print('Submitting variant CA')
                    # print(variantCA)
                    submitRequest(variantCA, False)
                    # print('submitted variant CA')
                    totalSubmission += 1

                    # US
                    variantUS = copy(variantCA)

                    variantUS['id'] = 'online:EN:US:' + prod['sku']
                    variantUS['link'] = variantURLUS
                    variantUS['targetCountry'] = 'US'
                    variantUS['price'] = {'value': round(prod['price']/1.1, 2), 'currency': 'USD'}
                    variantUS['customLabel0'] = productGeneral['customLabel0'] + '-US'
                    variantUS['customLabel0'] = 'test'


                    # Construct the service object to interact with the Content API.
                    # service, config, _ = common.init(argv, __doc__)

                    # # Get the merchant ID from merchant-info.json.
                    # merchant_id = config['merchantId']

                    # # print(variantUS)
                    # # Create the request with the merchant ID and product object.
                    # request = service.products().insert(merchantId=merchant_id, body=variantUS)

                    # # Execute the request and print the result.
                    # result = request.execute()
                    # print('Product with offerId "%s" was updated.' %
                    #       (result['offerId']))
                    if (json.loads(prod['custom_fields'])['googlemcexcludeus'] == False):
                        print('Submitting variant US')
                        submitRequest(variantUS, last)
                        totalSubmission += 1
                except:
                    print(prod['sku'] + " variant doesn't exist in BigC")
            else:
                if(prod['parent_product_code'] != ""):
                    print(prod['sku'] + "Variant should be on BigC but isn't")
                else:
                    # Single product
                    # CA
                    # singleProdBigCInfo = v3client.get('/catalog/products/' + str(prod['bigc_product_id']) + '/')
                    try:
                        singleProdBigCInfo = api.Products.get(prod['bigc_product_id'])
                        # print(singleProdBigCInfo)
                        # print(singleProdBigCInfo)
                        singleCA = copy(productGeneral)
                        singleCA['id'] = 'online:EN:CA:' + prod['sku']
                        # singleCA['link'] = 'https://safetymedia.com/' + prod['sku'] + '/' + json.loads(prod['custom_fields'])['seourl'].replace(' ', '-').replace("'","").replace('"','').replace(',','').lower() + '?setCurrencyId=1'
                        singleCA['link'] = 'https://safetymedia.com/' + prod['sku'] + '/' + fixURL(json.loads(prod['custom_fields'])['seourl']).lower() + '?setCurrencyId=1'
                        singleCA['imageLink'] =  singleProdBigCInfo['primary_image']['zoom_url']
                        singleCA['targetCountry'] = 'CA'
                        singleCA['price'] = {'value': prod['price'], 'currency': 'CAD'}
                        singleCA['customLabel0'] = productGeneral['customLabel0'] + '-CA'

                        # singleCA['itemGroupId'] = prod['parent_product_code']
                        # If gtin exists, set it
                        if(('upc' in prod['upcs_values']) and (json.loads(prod['upcs_values'])['upc'] != '')):
                            singleCA['gtin'] = json.loads(prod['upcs_values'])['upc']
                            singleCA['identifierExists'] = 'True'
                            # print('made it')
                        elif(('isbn' in prod['custom_fields']) and (json.loads(prod['custom_fields'])['isbn'] != '')):
                            singleCA['gtin'] = json.loads(prod['custom_fields'])['isbn']
                            singleCA['identifierExists'] = 'True'
                        # singleCA['mpn'] = 
                        # Construct the service object to interact with the Content API.
                        # service, config, _ = common.init(argv, __doc__)

                        # # Get the merchant ID from merchant-info.json.
                        # merchant_id = config['merchantId']

                        # print(singleCA)
                        # # Create the request with the merchant ID and product object.
                        # request = service.products().insert(merchantId=merchant_id, body=singleCA)

                        # # Execute the request and print the result.
                        # result = request.execute()
                        # print('Product with offerId "%s" was updated.' %
                        #       (result['offerId']))
                        submitRequest(singleCA, False)
                        totalSubmission += 1

                        # US
                        singleUS = copy(singleCA)

                        singleUS['id'] = 'online:EN:US:' + prod['sku']
                        singleUS['link'] = 'https://safetymedia.com/' + prod['sku'] + '/' + fixURL(json.loads(prod['custom_fields'])['seourl']).lower() + '?setCurrencyId=2'
                        singleUS['targetCountry'] = 'US'
                        singleUS['price'] = {'value': round(prod['price']/1.1, 2), 'currency': 'USD'}
                        singleUS['customLabel0'] = productGeneral['customLabel0'] + '-US'

                        # Construct the service object to interact with the Content API.
                        # service, config, _ = common.init(argv, __doc__)

                        # # Get the merchant ID from merchant-info.json.
                        # merchant_id = config['merchantId']

                        # # print(singleUS)
                        # # Create the request with the merchant ID and product object.
                        # # request = service.products().insert(merchantId=merchant_id, body=singleUS)
                        # request = service.products().insert(merchantId=merchant_id, body=singleUS)

                        # # Execute the request and print the result.
                        # result = request.execute()
                        # print('Product with offerId "%s" was updated.' %
                        #       (result['offerId']))
                        if ( (not 'googlemcexcludeus' in json.loads(prod['custom_fields'])) or (('googlemcexcludeus' in json.loads(prod['custom_fields'])) and (json.loads(prod['custom_fields'])['googlemcexcludeus'] == False))):
                            submitRequest(singleUS, last)
                            totalSubmission += 1
                    except:
                        print(prod['sku'] + 'Resource didnt exist in BigC')

        else:
            1
            # print('Parent')
        # exit()
    submitRequest('', True)
    print('Total submissions: ' + str(totalSubmission))
    quit()

        # once for CA, second time for US

        # print(prod['variant_id'])
        # product = {
        #     'offerId':
        #     prod['sku'],
        #     'id':
        #     'online:EN:CA:' + prod['sku'],
        #     'title':
        #     prod['product_name'],
        #     'description':
        #     custom_fields['prodhtml'],
        #     'link':
        #     'https://safetymedia.com/AA67/tamper-dye',
        #     'imageLink':
        #     'https://cdn11.bigcommerce.com/s-z84xkjcnbz/images/stencil/1920w/products/15839/86766/product16845__93436.1650403573.jpg?c=1',
        #     'contentLanguage':
        #     'en',
        #     'targetCountry':
        #     'CA',
        #     'channel':
        #     'online',
        #     'availability':
        #     'in stock',
        #     'condition':
        #     'new',
        #     'googleProductCategory':
        #     'Media > Books',
        #     'gtin':
        #     '9780007312896',
        #     'price': {
        #         'value': '85.59',
        #         'currency': 'CAD'
        #     },
        #     'shipping': [{
        #         'country': 'CA',
        #         'service': 'Standard shipping',
        #         'price': {
        #             'value': '1',
        #             'currency': 'CAD'
        #         }
        #     }],
        #     'shippingWeight': {
        #         'value': '200',
        #         'unit': 'grams'
        #     }
        # }
    
    

    # Construct the service object to interact with the Content API.
    service, config, _ = common.init(argv, __doc__)

    # Get the merchant ID from merchant-info.json.
    merchant_id = config['merchantId']

    # Create the request with the merchant ID and product object.
    request = service.products().insert(merchantId=merchant_id, body=product)

    # Execute the request and print the result.
    result = request.execute()
    print('Product with offerId "%s" was created.' % (result['offerId']))

    conn.close()

    productCA = {
        'offerId': prod['sku'],
        'id': 'online:EN:CA:' + prod['sku'],
        'title': prod['product_name'],
        'description': custom_fields['prodhtml'],
        'link': variantURLCA,
        'imageLink': variantBigCInfo['data']['image_url'],
        'contentLanguage': 'en',
        # 'targetCountry': '',
        'channel': 'online',
        'availability': 'in stock',
        'condition': 'new',
        'gtin': '9780007312896',
        'price': {
            'value': prod['price'],
            'currency': 'CAD'
        },
        'shipping': [{
            'country': 'CA',
            'service': 'Standard shipping',
            'price': {
                'value': '1',
                'currency': 'CAD'
            }
        }],
        'shippingWeight': {
            'value': '200',
            'unit': 'grams'
        }
    }


# Allow the function to be called with arguments passed from the command line.
if __name__ == '__main__':
    main(sys.argv)
